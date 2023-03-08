// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright contributors to the Ceph project
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#include "rgw_sal_posix.h"
#include "rgw_multi.h"
#include <dirent.h>
#include <sys/stat.h>
#include <sys/xattr.h>
#include <unistd.h>
#include "include/scope_guard.h"

#define dout_subsys ceph_subsys_rgw
#define dout_context g_ceph_context

namespace rgw { namespace sal {

const int64_t READ_SIZE = 8 * 1024;
char read_buf[READ_SIZE];
const std::string ATTR_PREFIX = "user.X-RGW-";
const std::string RGW_POSIX_ATTR_MPUPLOAD = "POSIX-Multipart-Upload";
const std::string mp_ns = "multipart";
const std::string MP_OBJ_PART_PFX = "part-";
const std::string MP_OBJ_PART_FMT = "{:0>5}";
const std::string MP_OBJ_HEAD_NAME = MP_OBJ_PART_PFX + "00000";


static inline User* nextUser(User* t)
{
  if (!t)
    return nullptr;

  return dynamic_cast<FilterUser*>(t)->get_next();
}

static inline std::string decode_name(const char* name)
{
  std::string decoded_name(name);
  return decoded_name;
}

static inline void bucket_statx_save(struct statx& stx, RGWBucketEnt& ent, ceph::real_time& mtime)
{
  mtime = ceph::real_clock::from_time_t(stx.stx_mtime.tv_sec);
  ent.creation_time = ceph::real_clock::from_time_t(stx.stx_btime.tv_sec);
  ent.size = stx.stx_size;
  ent.size_rounded = stx.stx_blocks * 512;
}

static inline int copy_dir_fd(int old_fd)
{
  return openat(old_fd, ".", O_RDONLY | O_DIRECTORY | O_NOFOLLOW);
}

static int get_x_attrs(optional_yield y, const DoutPrefixProvider* dpp, int fd,
		       Attrs& attrs, const std::string& display)
{
  char namebuf[64 * 1024]; // Max list size supported on linux
  ssize_t buflen;
  int ret;

  buflen = flistxattr(fd, namebuf, sizeof(namebuf));
  if (buflen < 0) {
    ret = errno;
    ldpp_dout(dpp, 0) << "ERROR: could not list attributes for " << display << ": "
      << cpp_strerror(ret) << dendl;
    return -ret;
  }

  char *keyptr = namebuf;
  while (buflen > 0) {
    std::string value;
    ssize_t vallen, keylen;
    char* vp;

    keylen = strlen(keyptr) + 1;
    std::string key(keyptr);
    std::string::size_type prefixloc = key.find(ATTR_PREFIX);

    if (prefixloc == std::string::npos) {
      /* Not one of our attributes */
      buflen -= keylen;
      keyptr += keylen;
      continue;
    }

    /* Make a key that has just the attribute name */
    key.erase(prefixloc, ATTR_PREFIX.length());

    vallen = fgetxattr(fd, keyptr, nullptr, 0);
    if (vallen < 0) {
      ret = errno;
      ldpp_dout(dpp, 0) << "ERROR: could not get attribute " << keyptr << " for " << display << ": " << cpp_strerror(ret) << dendl;
      return -ret;
    } else if (vallen == 0) {
      /* No attribute value for this name */
      buflen -= keylen;
      keyptr += keylen;
      continue;
    }

    value.reserve(vallen + 1);
    vp = &value[0];

    vallen = fgetxattr(fd, keyptr, vp, vallen);
    if (vallen < 0) {
      ret = errno;
      ldpp_dout(dpp, 0) << "ERROR: could not get attribute " << keyptr << " for " << display << ": " << cpp_strerror(ret) << dendl;
      return -ret;
    }

    bufferlist bl;
    bl.append(vp, vallen);
    attrs.emplace(std::move(key), std::move(bl)); /* key and bl are r-value refs */

    buflen -= keylen;
    keyptr += keylen;
  }

  return 0;
}

static int write_x_attr(const DoutPrefixProvider* dpp, optional_yield y, int fd,
			const std::string& key, bufferlist& value,
			const std::string& display)
{
  int ret;
  std::string attrname;

  attrname = ATTR_PREFIX + key;

  ret = fsetxattr(fd, attrname.c_str(), value.c_str(), value.length(), 0);
  if (ret < 0) {
    ret = errno;
    ldpp_dout(dpp, 0) << "ERROR: could not write attribute " << attrname << " for " << display << ": " << cpp_strerror(ret) << dendl;
    return -ret;
  }

  return 0;
}

static int delete_directory(int parent_fd, const char* dname, bool delete_children,
		     const DoutPrefixProvider* dpp)
{
  int ret;
  int dir_fd = -1;

  if (delete_children) {
    DIR* dir;
    struct dirent* entry;

    dir_fd = openat(parent_fd, dname, O_RDONLY | O_DIRECTORY | O_NOFOLLOW);
    if (dir_fd < 0) {
      dir_fd = errno;
      ldpp_dout(dpp, 0) << "ERROR: could not open subdir " << dname << ": "
	<< cpp_strerror(dir_fd) << dendl;
      return -dir_fd;
    }

    dir = fdopendir(dir_fd);
    if (dir == NULL) {
      ret = errno;
      ldpp_dout(dpp, 0) << "ERROR: could not open bucket " << dname << " for listing: "
	<< cpp_strerror(ret) << dendl;
      return -ret;
    }

    errno = 0;
    while ((entry = readdir(dir)) != NULL) {
      struct statx stx;

      if ((entry->d_name[0] == '.' && entry->d_name[1] == '\0')
	  || (entry->d_name[0] == '.' && entry->d_name[1] == '.'
	      && entry->d_name[2] == '\0')) {
	/* Skip . and .. */
	errno = 0;
	continue;
      }

      ret = statx(dir_fd, entry->d_name, AT_SYMLINK_NOFOLLOW, STATX_ALL, &stx);
      if (ret < 0) {
	ret = errno;
	ldpp_dout(dpp, 0) << "ERROR: could not stat object " << entry->d_name << ": "
	  << cpp_strerror(ret) << dendl;
	return -ret;
      }

      if (S_ISDIR(stx.stx_mode)) {
	/* Recurse */
	ret = delete_directory(dir_fd, entry->d_name, true, dpp);
	if (ret < 0) {
	  return ret;
	}

	continue;
      }

      /* Otherwise, unlink */
      ret = unlinkat(dir_fd, entry->d_name, 0);
      if (ret < 0) {
	ret = errno;
	ldpp_dout(dpp, 0) << "ERROR: could not remove file " << entry->d_name << ": "
	  << cpp_strerror(ret) << dendl;
	return -ret;
      }
    }
  }

  ret = unlinkat(parent_fd, dname, AT_REMOVEDIR);
  if (ret < 0) {
    ret = errno;
    if (errno != ENOENT) {
      ldpp_dout(dpp, 0) << "ERROR: could not remove bucket " << dname << ": "
	<< cpp_strerror(ret) << dendl;
      return -ret;
    }
  }

  return 0;
}

int POSIXDriver::initialize(CephContext *cct, const DoutPrefixProvider *dpp)
{
  FilterDriver::initialize(cct, dpp);

  base_path = g_conf().get_val<std::string>("rgw_posix_base_path");

  ldpp_dout(dpp, 20) << "Initializing POSIX driver: " << base_path << dendl;
  root_fd = openat(-1, base_path.c_str(), O_RDONLY | O_DIRECTORY | O_NOFOLLOW);
  if (root_fd == -1) {
    int err = errno;
    if (err == ENOTDIR) {
      ldpp_dout(dpp, 0) << " ERROR: base path (" << base_path
	<< "): was not a directory." << dendl;
      return -err;
    } else if (err == ENOENT) {
      err = mkdir(base_path.c_str(), S_IRWXU);
      if (err < 0) {
	err = errno;
	ldpp_dout(dpp, 0) << " ERROR: could not create base path ("
	  << base_path << "): " << cpp_strerror(err) << dendl;
	return -err;
      }
      root_fd = open(base_path.c_str(), O_RDONLY | O_DIRECTORY | O_NOFOLLOW);
    }
  }
  ldpp_dout(dpp, 20) << "root_fd: " << root_fd << dendl;
  if (root_fd == -1) {
    int err = errno;
    ldpp_dout(dpp, 0) << " ERROR: could not open base path ("
      << base_path << "): " << cpp_strerror(err) << dendl;
    return -err;
  }

  ldpp_dout(dpp, 20) << "SUCCESS" << dendl;
  return 0;
}

std::unique_ptr<User> POSIXDriver::get_user(const rgw_user &u)
{
  std::unique_ptr<User> user = next->get_user(u);

  return std::make_unique<POSIXUser>(std::move(user), this);
}

int POSIXDriver::get_user_by_access_key(const DoutPrefixProvider* dpp, const std::string& key, optional_yield y, std::unique_ptr<User>* user)
{
  std::unique_ptr<User> nu;
  int ret;

  ret = next->get_user_by_access_key(dpp, key, y, &nu);
  if (ret != 0)
    return ret;

  User* u = new POSIXUser(std::move(nu), this);
  user->reset(u);
  return 0;
}

int POSIXDriver::get_user_by_email(const DoutPrefixProvider* dpp, const std::string& email, optional_yield y, std::unique_ptr<User>* user)
{
  std::unique_ptr<User> nu;
  int ret;

  ret = next->get_user_by_email(dpp, email, y, &nu);
  if (ret != 0)
    return ret;

  User* u = new POSIXUser(std::move(nu), this);
  user->reset(u);
  return 0;
}

int POSIXDriver::get_user_by_swift(const DoutPrefixProvider* dpp, const std::string& user_str, optional_yield y, std::unique_ptr<User>* user)
{
  std::unique_ptr<User> nu;
  int ret;

  ret = next->get_user_by_swift(dpp, user_str, y, &nu);
  if (ret != 0)
    return ret;

  User* u = new POSIXUser(std::move(nu), this);
  user->reset(u);
  return 0;
}

std::unique_ptr<Object> POSIXDriver::get_object(const rgw_obj_key& k)
{
  return std::make_unique<POSIXObject>(this, k);
}

int POSIXDriver::get_bucket(const DoutPrefixProvider* dpp, User* u, const rgw_bucket& b, std::unique_ptr<Bucket>* bucket, optional_yield y)
{
  int ret;
  Bucket* bp;

  bp = new POSIXBucket(this, root_fd, b, u);
  ret = bp->load_bucket(dpp, y);
  if (ret < 0) {
    delete bp;
    return ret;
  }

  bucket->reset(bp);
  return 0;
}

int POSIXDriver::get_bucket(User* u, const RGWBucketInfo& i, std::unique_ptr<Bucket>* bucket)
{
  Bucket* bp;

  bp = new POSIXBucket(this, root_fd, i, u);
  /* Don't need to fetch the bucket info, use the provided one */

  bucket->reset(bp);
  return 0;
}

int POSIXDriver::get_bucket(const DoutPrefixProvider* dpp, User* u, const std::string& tenant, const std::string& name, std::unique_ptr<Bucket>* bucket, optional_yield y)
{
  rgw_bucket b;

  b.tenant = tenant;
  b.name = name;

  return get_bucket(dpp, u, b, bucket, y);
}

std::unique_ptr<Writer> POSIXDriver::get_append_writer(const DoutPrefixProvider *dpp,
				  optional_yield y,
				  std::unique_ptr<rgw::sal::Object> _head_obj,
				  const rgw_user& owner,
				  const rgw_placement_rule *ptail_placement_rule,
				  const std::string& unique_tag,
				  uint64_t position,
				  uint64_t *cur_accounted_size)
{
  std::unique_ptr<Object> no;

  std::unique_ptr<Writer> writer = next->get_append_writer(dpp, y, std::move(no),
							   owner, ptail_placement_rule,
							   unique_tag, position,
							   cur_accounted_size);

  return std::make_unique<FilterWriter>(std::move(writer), std::move(_head_obj));
}

std::unique_ptr<Writer> POSIXDriver::get_atomic_writer(const DoutPrefixProvider *dpp,
				  optional_yield y,
				  std::unique_ptr<rgw::sal::Object> _head_obj,
				  const rgw_user& owner,
				  const rgw_placement_rule *ptail_placement_rule,
				  uint64_t olh_epoch,
				  const std::string& unique_tag)
{

  return std::make_unique<POSIXAtomicWriter>(dpp, y, std::move(_head_obj), this, owner, ptail_placement_rule, olh_epoch, unique_tag);
}

void POSIXDriver::finalize(void)
{
  next->finalize();
}

void POSIXDriver::register_admin_apis(RGWRESTMgr* mgr)
{
  return next->register_admin_apis(mgr);
}

std::unique_ptr<Notification> POSIXDriver::get_notification(rgw::sal::Object* obj,
			      rgw::sal::Object* src_obj, struct req_state* s,
			      rgw::notify::EventType event_type, optional_yield y,
			      const std::string* object_name)
{
  return next->get_notification(obj, src_obj, s, event_type, y, object_name);
}

std::unique_ptr<Notification> POSIXDriver::get_notification(const DoutPrefixProvider* dpp,
                              rgw::sal::Object* obj, rgw::sal::Object* src_obj,
                              rgw::notify::EventType event_type,
                              rgw::sal::Bucket* _bucket,
                              std::string& _user_id, std::string& _user_tenant,
                              std::string& _req_id, optional_yield y)
{
  return next->get_notification(dpp, obj, src_obj, event_type, _bucket, _user_id, _user_tenant, _req_id, y);
}

int POSIXDriver::close()
{
  if (root_fd < 0) {
    return 0;
  }

  ::close(root_fd);
  root_fd = -1;

  return 0;
}

int POSIXUser::list_buckets(const DoutPrefixProvider* dpp, const std::string& marker,
			     const std::string& end_marker, uint64_t max,
			     bool need_stats, BucketList &buckets, optional_yield y)
{
  DIR* dir;
  struct dirent* entry;
  int dfd;
  int ret;

  buckets.clear();

  /* it's not sufficient to dup(root_fd), as as the new fd would share
   * the file position of root_fd */
  dfd = copy_dir_fd(driver->get_root_fd());
  if (dfd == -1) {
    ret = errno;
    ldpp_dout(dpp, 0) << "ERROR: could not open root to list buckets: "
      << cpp_strerror(ret) << dendl;
    return -errno;
  }

  dir = fdopendir(dfd);
  if (dir == NULL) {
    ret = errno;
    ldpp_dout(dpp, 0) << "ERROR: could not open root to list buckets: "
      << cpp_strerror(ret) << dendl;
    close(dfd);
    return -ret;
  }

  auto cleanup_guard = make_scope_guard(
    [&dir]
      {
	closedir(dir);
	// dfd is also closed
      }
    );

  errno = 0;
  while ((entry = readdir(dir)) != NULL) {
    struct statx stx;

    ret = statx(driver->get_root_fd(), entry->d_name, AT_SYMLINK_NOFOLLOW, STATX_ALL, &stx);
    if (ret < 0) {
      ret = errno;
      ldpp_dout(dpp, 0) << "ERROR: could not stat object " << entry->d_name << ": "
	<< cpp_strerror(ret) << dendl;
      buckets.clear();
      return -ret;
    }

    if (!S_ISDIR(stx.stx_mode)) {
      /* Not a bucket, skip it */
      errno = 0;
      continue;
    }
    if (entry->d_name[0] == '.') {
      /* Skip dotfiles */
      errno = 0;
      continue;
    }

    /* TODO Use stat_to_ent */
    //RGWBucketEnt ent;
    //ent.bucket.name = decode_name(entry->d_name);
    //bucket_statx_save(stx, ent, mtime);
    RGWBucketInfo info;
    info.bucket.name = decode_name(entry->d_name);
    info.owner.id = std::to_string(stx.stx_uid); // TODO convert to owner name
    info.creation_time = ceph::real_clock::from_time_t(stx.stx_btime.tv_sec);

    std::unique_ptr<rgw::sal::Bucket> bucket;
    ret = driver->get_bucket(this, info, &bucket);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "ERROR: could not get bucket " << info.bucket << ": "
	<< cpp_strerror(ret) << dendl;
      buckets.clear();
      return -ret;
    }

    buckets.add(std::move(bucket));

    errno = 0;
  }
  ret = errno;
  if (ret != 0) {
    ldpp_dout(dpp, 0) << "ERROR: could not list buckets for " << get_display_name() << ": "
      << cpp_strerror(ret) << dendl;
    buckets.clear();
    return -ret;
  }

  return 0;
}

int POSIXUser::create_bucket(const DoutPrefixProvider* dpp,
			      const rgw_bucket& b,
			      const std::string& zonegroup_id,
			      rgw_placement_rule& placement_rule,
			      std::string& swift_ver_location,
			      const RGWQuotaInfo * pquota_info,
			      const RGWAccessControlPolicy& policy,
			      Attrs& attrs,
			      RGWBucketInfo& info,
			      obj_version& ep_objv,
			      bool exclusive,
			      bool obj_lock_enabled,
			      bool* existed,
			      req_info& req_info,
			      std::unique_ptr<Bucket>* bucket_out,
			      optional_yield y)
{
  info.bucket = b;
  POSIXBucket* fb = new POSIXBucket(driver, driver->get_root_fd(), info, this);

  int ret = fb->create(dpp, y, existed);
  if (ret < 0) {
    delete fb;
    return  ret;
  }

  ret = fb->set_attrs(attrs);
  if (ret < 0) {
    delete fb;
    return  ret;
  }

  bucket_out->reset(fb);
  return 0;
}

int POSIXUser::read_attrs(const DoutPrefixProvider* dpp, optional_yield y)
{
  return next->read_attrs(dpp, y);
}

int POSIXUser::merge_and_store_attrs(const DoutPrefixProvider* dpp,
				      Attrs& new_attrs, optional_yield y)
{
  return next->merge_and_store_attrs(dpp, new_attrs, y);
}

int POSIXUser::load_user(const DoutPrefixProvider* dpp, optional_yield y)
{
  return next->load_user(dpp, y);
}

int POSIXUser::store_user(const DoutPrefixProvider* dpp, optional_yield y, bool exclusive, RGWUserInfo* old_info)
{
  return next->store_user(dpp, y, exclusive, old_info);
}

int POSIXUser::remove_user(const DoutPrefixProvider* dpp, optional_yield y)
{
  return next->remove_user(dpp, y);
}

std::unique_ptr<Object> POSIXBucket::get_object(const rgw_obj_key& k)
{
  return std::make_unique<POSIXObject>(driver, k, this);
}

int POSIXBucket::list(const DoutPrefixProvider* dpp, ListParams& params, int max,
		       ListResults& results, optional_yield y)
{
  int ret = for_each(dpp, [this, &results, &dpp](const char* name) {
    int ret;
    struct statx stx;

    if (name[0] == '.') {
      /* Skip dotfiles */
      return 0;
    }

    ret = statx(dir_fd, name, AT_SYMLINK_NOFOLLOW, STATX_ALL, &stx);
    if (ret < 0) {
      ret = errno;
      ldpp_dout(dpp, 0) << "ERROR: could not stat object " << name << ": "
	<< cpp_strerror(ret) << dendl;
      results.objs.clear();
      return -ret;
    }

    if (S_ISREG(stx.stx_mode) || S_ISDIR(stx.stx_mode)) {
      rgw_bucket_dir_entry *e = new rgw_bucket_dir_entry;
      e->key.name = decode_name(name);
      e->ver.pool = 1;
      e->ver.epoch = 1;
      e->exists = true;
      e->meta.category = RGWObjCategory::Main;
      e->meta.size = stx.stx_size;
      e->meta.mtime = ceph::real_clock::from_time_t(stx.stx_mtime.tv_sec);
      e->meta.owner = std::to_string(stx.stx_uid); // TODO convert to owner name
      e->meta.owner_display_name = std::to_string(stx.stx_uid); // TODO convert to owner name
      e->meta.accounted_size = stx.stx_blksize * stx.stx_blocks; // TODO Won't work for mpobj
      e->meta.storage_class = RGW_STORAGE_CLASS_STANDARD;
      e->meta.appendable = true;

      results.objs.push_back(*e);
    }

    return 0;
  });
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: could not list bucket " << get_name() << ": "
      << cpp_strerror(ret) << dendl;
    results.objs.clear();
    return ret;
  }

  return 0;
}

int POSIXBucket::merge_and_store_attrs(const DoutPrefixProvider* dpp,
					Attrs& new_attrs, optional_yield y)
{
  for(auto& it : new_attrs) {
	  attrs[it.first] = it.second;
  }
  /* TODO store attributes */
  return 0;
}

int POSIXBucket::remove_bucket(const DoutPrefixProvider* dpp,
				bool delete_children,
				bool forward_to_master,
				req_info* req_info,
				optional_yield y)
{
  return delete_directory(parent_fd, get_fname().c_str(),
			  delete_children, dpp);
}

int POSIXBucket::remove_bucket_bypass_gc(int concurrent_max,
					 bool keep_index_consistent,
					 optional_yield y,
					 const DoutPrefixProvider *dpp)
{
  return remove_bucket(dpp, true, false, nullptr, y);
}

int POSIXBucket::load_bucket(const DoutPrefixProvider* dpp, optional_yield y,
			      bool get_stats)
{
  int ret;

  if (get_name()[0] == '.') {
    /* Skip dotfiles */
    return -ERR_INVALID_OBJECT_NAME;
  }
  ret = stat(dpp);
  if (ret < 0) {
    return ret;
  }

  bucket_statx_save(stx, ent, mtime);
  info.creation_time = ent.creation_time;

  if (owner) {
    info.owner = owner->get_id();
  }

  get_x_attrs(y, dpp, dir_fd, attrs, get_name());

  return 0;
}

int POSIXBucket::set_acl(const DoutPrefixProvider* dpp,
			 RGWAccessControlPolicy& acl,
			 optional_yield y)
{
  bufferlist aclbl;

  acls = acl;
  acl.encode(aclbl);

  attrs[RGW_ATTR_ACL] = aclbl;
  info.owner = acl.get_owner().get_id();

  return 0;
}

int POSIXBucket::read_stats(const DoutPrefixProvider *dpp,
			    const bucket_index_layout_generation& idx_layout,
			    int shard_id, std::string* bucket_ver, std::string* master_ver,
			    std::map<RGWObjCategory, RGWStorageStats>& stats,
			    std::string* max_marker, bool* syncstopped)
{
  return 0;
}

int POSIXBucket::read_stats_async(const DoutPrefixProvider *dpp,
				  const bucket_index_layout_generation& idx_layout,
				  int shard_id, RGWGetBucketStats_CB* ctx)
{
  return 0;
}

int POSIXBucket::sync_user_stats(const DoutPrefixProvider *dpp, optional_yield y)
{
  return 0;
}

int POSIXBucket::update_container_stats(const DoutPrefixProvider* dpp)
{
  /* Force re-stat */
  stat_done = false;
  int ret = stat(dpp);
  if (ret < 0) {
    return ret;
  }

  bucket_statx_save(stx, ent, mtime);
  info.creation_time = ent.creation_time;

  return 0;
}

int POSIXBucket::check_bucket_shards(const DoutPrefixProvider* dpp)
{
      return 0;
}

int POSIXBucket::chown(const DoutPrefixProvider* dpp, User& new_user, optional_yield y)
{
  /* TODO map user to UID/GID, and change it */
  return 0;
}

int POSIXBucket::put_info(const DoutPrefixProvider* dpp, bool exclusive, ceph::real_time _mtime)
{
  mtime = _mtime;

  struct timespec ts[2];
  ts[0].tv_nsec = UTIME_OMIT;
  ts[1] = ceph::real_clock::to_timespec(mtime);
  int ret = utimensat(parent_fd, get_fname().c_str(), ts, AT_SYMLINK_NOFOLLOW);
  if (ret < 0) {
    ret = errno;
    ldpp_dout(dpp, 0) << "ERROR: could not set mtime on bucket " << get_name() << ": "
      << cpp_strerror(ret) << dendl;
    return -ret;
  }

  ret = open(dpp);
  if (ret < 0) {
    return ret;
  }

  for (auto& it : attrs) {
	  int ret = write_x_attr(dpp, null_yield, dir_fd, it.first, it.second, get_name());
	  if (ret < 0) {
	    return ret;
	  }
  }
  return 0;
}

int POSIXBucket::check_empty(const DoutPrefixProvider* dpp, optional_yield y)
{
  DIR* dir;
  struct dirent* entry;
  int ret;

  ret = open(dpp);
  if (ret < 0) {
    return ret;
  }

  dir = fdopendir(dir_fd);
  if (dir == NULL) {
    ret = errno;
    ldpp_dout(dpp, 0) << "ERROR: could not open bucket " << get_name() << " for listing: "
      << cpp_strerror(ret) << dendl;
    return -ret;
  }

  errno = 0;
  while ((entry = readdir(dir)) != NULL) {
    if (entry->d_name[0] != '.') {
      return -ENOTEMPTY;
    }
    if (entry->d_name[1] == '.' || entry->d_name[1] == '\0') {
      continue;
    }
  }
  return 0;
}

int POSIXBucket::check_quota(const DoutPrefixProvider *dpp, RGWQuota& quota, uint64_t obj_size,
				optional_yield y, bool check_size_only)
{
    return 0;
}

int POSIXBucket::try_refresh_info(const DoutPrefixProvider* dpp, ceph::real_time* pmtime)
{
  int ret = update_container_stats(dpp);
  if (ret < 0) {
    return ret;
  }

  *pmtime = mtime;
  /* TODO Get attributes */
  return 0;
}

int POSIXBucket::read_usage(const DoutPrefixProvider *dpp, uint64_t start_epoch,
			    uint64_t end_epoch, uint32_t max_entries,
			    bool* is_truncated, RGWUsageIter& usage_iter,
			    std::map<rgw_user_bucket, rgw_usage_log_entry>& usage)
{
  return 0;
}

int POSIXBucket::trim_usage(const DoutPrefixProvider *dpp, uint64_t start_epoch, uint64_t end_epoch)
{
  return 0;
}

int POSIXBucket::remove_objs_from_index(const DoutPrefixProvider *dpp, std::list<rgw_obj_index_key>& objs_to_unlink)
{
  return 0;
}

int POSIXBucket::check_index(const DoutPrefixProvider *dpp, std::map<RGWObjCategory, RGWStorageStats>& existing_stats, std::map<RGWObjCategory, RGWStorageStats>& calculated_stats)
{
  return 0;
}

int POSIXBucket::rebuild_index(const DoutPrefixProvider *dpp)
{
  return 0;
}

int POSIXBucket::set_tag_timeout(const DoutPrefixProvider *dpp, uint64_t timeout)
{
  return 0;
}

int POSIXBucket::purge_instance(const DoutPrefixProvider* dpp)
{
  return 0;
}

std::unique_ptr<MultipartUpload> POSIXBucket::get_multipart_upload(
				  const std::string& oid,
				  std::optional<std::string> upload_id,
				  ACLOwner owner, ceph::real_time mtime)
{
  return std::make_unique<POSIXMultipartUpload>(driver, this, oid, upload_id, owner, mtime);
}

int POSIXBucket::list_multiparts(const DoutPrefixProvider *dpp,
				  const std::string& prefix,
				  std::string& marker,
				  const std::string& delim,
				  const int& max_uploads,
				  std::vector<std::unique_ptr<MultipartUpload>>& uploads,
				  std::map<std::string, bool> *common_prefixes,
				  bool *is_truncated)
{
  //std::vector<std::unique_ptr<MultipartUpload>> nup;
  //int ret;
//
  //ret = next->list_multiparts(dpp, prefix, marker, delim, max_uploads, nup,
			      //common_prefixes, is_truncated);
  //if (ret < 0)
    //return ret;
//
  //for (auto& ent : nup) {
    //uploads.emplace_back(std::make_unique<POSIXMultipartUpload>(std::move(ent), this, driver));
  //}

  return 0;
}

int POSIXBucket::abort_multiparts(const DoutPrefixProvider* dpp, CephContext* cct)
{
  return 0;
}

int POSIXBucket::create(const DoutPrefixProvider* dpp, optional_yield y, bool* existed)
{
  int ret = mkdirat(parent_fd, get_fname().c_str(), S_IRWXU);
  if (ret < 0) {
    ret = errno;
    if (ret != EEXIST) {
      if (dpp)
	ldpp_dout(dpp, 0) << "ERROR: could not create bucket " << get_name() << ": "
	  << cpp_strerror(ret) << dendl;
      return -ret;
    } else if (existed != nullptr) {
      *existed = true;
    }
  }

  return open(dpp);
}

std::string POSIXBucket::get_fname()
{
  std::string name;

  if (ns)
    name = "." + *ns + "_" + get_name();
  else
    name = get_name();

  return name;
}

int POSIXBucket::get_shadow_bucket(const DoutPrefixProvider* dpp, optional_yield y,
				   const std::string& ns,
				   const std::string& tenant, const std::string& name,
				   bool create, std::unique_ptr<POSIXBucket>* shadow)
{
  std::optional<std::string> ons{std::nullopt};
  int ret;
  POSIXBucket* bp;
  rgw_bucket b;

  b.tenant = tenant;
  b.name = name;

  if (!ns.empty()) {
    ons = ns;
  }

  open(dpp);

  bp = new POSIXBucket(driver, dir_fd, b, owner, ons);
  ret = bp->load_bucket(dpp, y);
  if (ret == -ENOENT && create) {
    /* Create it if it doesn't exist */
    ret = bp->create(dpp, y, nullptr);
  }
  if (ret < 0) {
    delete bp;
    return ret;
  }

  shadow->reset(bp);
  return 0;
}

template <typename F>
int POSIXBucket::for_each(const DoutPrefixProvider* dpp, const F func)
{
  DIR* dir;
  struct dirent* entry;
  int ret;

  ret = open(dpp);
  if (ret < 0) {
    return ret;
  }

  dir = fdopendir(dir_fd);
  if (dir == NULL) {
    ret = errno;
    ldpp_dout(dpp, 0) << "ERROR: could not open bucket " << get_name() << " for listing: "
      << cpp_strerror(ret) << dendl;
    return -ret;
  }

  rewinddir(dir);

  while ((entry = readdir(dir)) != NULL) {
    int r = func(entry->d_name);
    if (r < 0) {
      ret = r;
    }
  }
  return ret;

  return 0;
}

int POSIXBucket::open(const DoutPrefixProvider* dpp)
{
  if (dir_fd >= 0) {
    return 0;
  }

  int ret = openat(parent_fd, get_fname().c_str(),
		   O_RDONLY | O_DIRECTORY | O_NOFOLLOW);
  if (ret < 0) {
    ret = errno;
    ldpp_dout(dpp, 0) << "ERROR: could not open bucket " << get_name() << ": "
                  << cpp_strerror(ret) << dendl;
    return -ret;
  }

  dir_fd = ret;

  return 0;
}

// This is for renaming a shadow bucket to a MP object.  It won't work work for a normal bucket
int POSIXBucket::rename(const DoutPrefixProvider* dpp, optional_yield y, Object* target_obj)
{
  POSIXObject *to = dynamic_cast<POSIXObject*>(target_obj);
  POSIXBucket *tb = dynamic_cast<POSIXBucket*>(target_obj->get_bucket());

  // swap and delete
  int ret = renameat2(tb->get_dir_fd(dpp), get_fname().c_str(), tb->get_dir_fd(dpp), to->get_fname().c_str(), RENAME_EXCHANGE);
  if(ret < 0) {
    ret = errno;
    ldpp_dout(dpp, 0) << "ERROR: renameat2 for shadow object could not finish: "
	<< cpp_strerror(ret) << dendl;
    return -ret;
  }

  remove_bucket(dpp, true, false, nullptr, y);

  return 0;
}

int POSIXBucket::close()
{
  if (dir_fd < 0) {
    return 0;
  }

  ::close(dir_fd);
  dir_fd = -1;

  return 0;
}

int POSIXBucket::stat(const DoutPrefixProvider* dpp)
{
  if (stat_done) {
    return 0;
  }

  int ret = statx(parent_fd, get_fname().c_str(), AT_SYMLINK_NOFOLLOW,
		  STATX_ALL, &stx);
  if (ret < 0) {
    ret = errno;
    ldpp_dout(dpp, 0) << "ERROR: could not stat bucket " << get_name() << ": "
                  << cpp_strerror(ret) << dendl;
    return -ret;
  }
  if (!S_ISDIR(stx.stx_mode)) {
    /* Not a bucket */
    return -EINVAL;
  }

  stat_done = true;
  return 0;
}

int POSIXObject::delete_object(const DoutPrefixProvider* dpp,
				optional_yield y,
				bool prevent_versioning)
{
  POSIXBucket *b = dynamic_cast<POSIXBucket*>(get_bucket());
  if (!b) {
      ldpp_dout(dpp, 0) << "ERROR: could not get bucket for " << get_name() << dendl;
      return -EINVAL;
  }

  int ret = unlinkat(b->get_dir_fd(dpp), get_fname().c_str(), 0);
  if (ret < 0) {
    ret = errno;
    if (errno != ENOENT) {
      ldpp_dout(dpp, 0) << "ERROR: could not remove object " << get_name() << ": "
	<< cpp_strerror(ret) << dendl;
      return -ret;
    }
  }
  return 0;
}

int POSIXObject::delete_obj_aio(const DoutPrefixProvider* dpp, RGWObjState* astate,
				Completions* aio, bool keep_index_consistent,
				optional_yield y)
{
  /* Appears to be unused? */
  return delete_object(dpp, y, false);
}

int POSIXObject::copy_object(User* user,
                              req_info* info,
                              const rgw_zone_id& source_zone,
                              rgw::sal::Object* dest_object,
                              rgw::sal::Bucket* dest_bucket,
                              rgw::sal::Bucket* src_bucket,
                              const rgw_placement_rule& dest_placement,
                              ceph::real_time* src_mtime,
                              ceph::real_time* mtime,
                              const ceph::real_time* mod_ptr,
                              const ceph::real_time* unmod_ptr,
                              bool high_precision_time,
                              const char* if_match,
                              const char* if_nomatch,
                              AttrsMod attrs_mod,
                              bool copy_if_newer,
                              Attrs& attrs,
                              RGWObjCategory category,
                              uint64_t olh_epoch,
                              boost::optional<ceph::real_time> delete_at,
                              std::string* version_id,
                              std::string* tag,
                              std::string* etag,
                              void (*progress_cb)(off_t, void *),
                              void* progress_data,
                              const DoutPrefixProvider* dpp,
                              optional_yield y)
{
  POSIXBucket *db = dynamic_cast<POSIXBucket*>(dest_bucket);
  POSIXBucket *sb = dynamic_cast<POSIXBucket*>(src_bucket);

  if (!db || !sb) {
      ldpp_dout(dpp, 0) << "ERROR: could not get bucket to copy " << get_name() << dendl;
      return -EINVAL;
  }

  /* TODO Open and copy; set attrs */
  return 0;
}

int POSIXObject::get_obj_state(const DoutPrefixProvider* dpp, RGWObjState **pstate, optional_yield y, bool follow_olh)
{
  int ret = stat(dpp);
  if (ret < 0) {
    return ret;
  }
  *pstate = &state;

  return 0;
}

int POSIXObject::set_obj_attrs(const DoutPrefixProvider* dpp, Attrs* setattrs,
                            Attrs* delattrs, optional_yield y)
{
  if (delattrs) {
    for (auto& it : *delattrs) {
      state.attrset.erase(it.first);
    }
  }
  if (setattrs) {
    for (auto& it : *setattrs) {
      state.attrset[it.first] = it.second;
    }
  }

  for (auto& it : state.attrset) {
	  int ret = write_attr(dpp, y, it.first, it.second);
	  if (ret < 0) {
	    return ret;
	  }
  }
  return 0;
}

int POSIXObject::get_obj_attrs(optional_yield y, const DoutPrefixProvider* dpp,
                                rgw_obj* target_obj)
{
  int ret = open(dpp);
  if (ret < 0) {
    return ret;
  }

  return get_x_attrs(y, dpp, obj_fd, state.attrset, get_name());
}

int POSIXObject::modify_obj_attrs(const char* attr_name, bufferlist& attr_val,
                               optional_yield y, const DoutPrefixProvider* dpp)
{
  state.attrset[attr_name] = attr_val;
  /* TODO Write out attrs */
  return 0;
}

int POSIXObject::delete_obj_attrs(const DoutPrefixProvider* dpp, const char* attr_name,
                               optional_yield y)
{
  state.attrset.erase(attr_name);
  /* TODO Write out attrs */
  return 0;
}

bool POSIXObject::is_expired()
{
  auto iter = state.attrset.find(RGW_ATTR_DELETE_AT);
  if (iter != state.attrset.end()) {
    utime_t delete_at;
    try {
      auto bufit = iter->second.cbegin();
      decode(delete_at, bufit);
    } catch (buffer::error& err) {
      ldout(driver->ctx(), 0) << "ERROR: " << __func__ << ": failed to decode " RGW_ATTR_DELETE_AT " attr" << dendl;
      return false;
    }

    if (delete_at <= ceph_clock_now() && !delete_at.is_zero()) {
      return true;
    }
  }

  return false;
}

void POSIXObject::gen_rand_obj_instance_name()
{
  enum { OBJ_INSTANCE_LEN = 32 };
  char buf[OBJ_INSTANCE_LEN + 1];

  gen_rand_alphanumeric_no_underscore(driver->ctx(), buf, OBJ_INSTANCE_LEN);
  state.obj.key.set_instance(buf);
}

std::unique_ptr<MPSerializer> POSIXObject::get_serializer(const DoutPrefixProvider *dpp, const std::string& lock_name)
{
  return std::make_unique<MPPOSIXSerializer>(dpp, driver, this, lock_name);
}

int POSIXObject::transition(Bucket* bucket,
			    const rgw_placement_rule& placement_rule,
			    const real_time& mtime,
			    uint64_t olh_epoch,
			    const DoutPrefixProvider* dpp,
			    optional_yield y)
{
  return -ERR_NOT_IMPLEMENTED;
}

int POSIXObject::transition_to_cloud(Bucket* bucket,
			   rgw::sal::PlacementTier* tier,
			   rgw_bucket_dir_entry& o,
			   std::set<std::string>& cloud_targets,
			   CephContext* cct,
			   bool update_object,
			   const DoutPrefixProvider* dpp,
			   optional_yield y)
{
  return -ERR_NOT_IMPLEMENTED;
}

bool POSIXObject::placement_rules_match(rgw_placement_rule& r1, rgw_placement_rule& r2)
{
  return (r1 == r2);
}

int POSIXObject::dump_obj_layout(const DoutPrefixProvider *dpp, optional_yield y, Formatter* f)
{
    return 0;
}

int POSIXObject::swift_versioning_restore(bool& restored,
				       const DoutPrefixProvider* dpp)
{
  return 0;
}

int POSIXObject::swift_versioning_copy(const DoutPrefixProvider* dpp,
				    optional_yield y)
{
  return 0;
}

int POSIXObject::omap_get_vals(const DoutPrefixProvider *dpp, const std::string& marker, uint64_t count,
				  std::map<std::string, bufferlist> *m,
				  bool* pmore, optional_yield y)
{
  /* TODO Figure out omap */
  return 0;
}

int POSIXObject::omap_get_all(const DoutPrefixProvider *dpp, std::map<std::string, bufferlist> *m,
				 optional_yield y)
{
  /* TODO Figure out omap */
  return 0;
}

int POSIXObject::omap_get_vals_by_keys(const DoutPrefixProvider *dpp, const std::string& oid,
					  const std::set<std::string>& keys,
					  Attrs* vals)
{
  /* TODO Figure out omap */
  return 0;
}

int POSIXObject::omap_set_val_by_key(const DoutPrefixProvider *dpp, const std::string& key, bufferlist& val,
					bool must_exist, optional_yield y)
{
  /* TODO Figure out omap */
  return 0;
}

int POSIXObject::chown(User& new_user, const DoutPrefixProvider* dpp, optional_yield y)
{
  POSIXBucket *b = dynamic_cast<POSIXBucket*>(get_bucket());
  if (!b) {
      ldpp_dout(dpp, 0) << "ERROR: could not get bucket for " << get_name() << dendl;
      return -EINVAL;
  }
  /* TODO Get UID from user */
  int uid = 0;
  int gid = 0;

  int ret = fchownat(b->get_dir_fd(dpp), get_fname().c_str(), uid, gid, AT_SYMLINK_NOFOLLOW);
  if (ret < 0) {
    ret = errno;
    ldpp_dout(dpp, 0) << "ERROR: could not remove object " << get_name() << ": "
      << cpp_strerror(ret) << dendl;
    return -ret;
    }

  return 0;
}

int POSIXObject::stat(const DoutPrefixProvider* dpp)
{
  if (stat_done) {
    return 0;
  }

  POSIXBucket *b = dynamic_cast<POSIXBucket*>(get_bucket());
  if (!b) {
      ldpp_dout(dpp, 0) << "ERROR: could not get bucket for " << get_name() << dendl;
      return -EINVAL;
  }

  int ret = statx(b->get_dir_fd(dpp), get_fname().c_str(), AT_SYMLINK_NOFOLLOW,
		  STATX_ALL, &stx);
  if (ret < 0) {
    ret = errno;
    ldpp_dout(dpp, 0) << "ERROR: could not stat object " << get_name() << ": "
                  << cpp_strerror(ret) << dendl;
    return -ret;
  }
  if (S_ISREG(stx.stx_mode)) {
    /* Normal object */
    state.accounted_size = state.size = stx.stx_size;
    state.mtime = ceph::real_clock::from_time_t(stx.stx_mtime.tv_sec);
  } else if (S_ISDIR(stx.stx_mode)) {
    /* multipart object */
    /* Get the shadow bucket */
    POSIXBucket* pb = dynamic_cast<POSIXBucket*>(bucket);
    ret = pb->get_shadow_bucket(nullptr, null_yield, std::string(),
				std::string(), get_fname(), false, &shadow);
    if (ret < 0) {
      return ret;
    }

    state.mtime = ceph::real_clock::from_time_t(stx.stx_mtime.tv_sec);
    /* Add up size of parts */
    uint64_t total_size{0};
    int fd = shadow->get_dir_fd(dpp);
    shadow->for_each(dpp, [this, &total_size, fd, &dpp](const char* name) {
      int ret;
      struct statx stx;
      std::string sname = name;

      if (sname.rfind(MP_OBJ_PART_PFX, 0) != 0) {
	/* Skip non-parts */
	return 0;
      }

      ret = statx(fd, name, AT_SYMLINK_NOFOLLOW, STATX_ALL, &stx);
      if (ret < 0) {
	ret = errno;
	ldpp_dout(dpp, 0) << "ERROR: could not stat object " << name << ": " << cpp_strerror(ret) << dendl;
	return -ret;
      }

      if (!S_ISREG(stx.stx_mode)) {
	/* Skip non-files */
	return 0;
      }

      parts[name] = stx.stx_size;
      total_size += stx.stx_size;
      return 0;
      });
    state.accounted_size = state.size = total_size;
  } else {
    /* Not an object */
    return -EINVAL;
  }

  stat_done = true;
  state.exists = true;

  return 0;
}

std::unique_ptr<Object::ReadOp> POSIXObject::get_read_op()
{
  return std::make_unique<POSIXReadOp>(this);
}

std::unique_ptr<Object::DeleteOp> POSIXObject::get_delete_op()
{
  return std::make_unique<POSIXDeleteOp>(this);
}

int POSIXObject::open(const DoutPrefixProvider* dpp, bool temp_file)
{
  if (obj_fd >= 0) {
    return 0;
  }

  if (shadow) {
    obj_fd = shadow->get_dir_fd(dpp);
    return obj_fd;
  }

  POSIXBucket *b = dynamic_cast<POSIXBucket*>(get_bucket());
  if (!b) {
      ldpp_dout(dpp, 0) << "ERROR: could not get bucket for " << get_name() << dendl;
      return -EINVAL;
  }

  int ret, flags;
  std::string path;

  if(temp_file) {
    flags = O_TMPFILE | O_RDWR;
    path = ".";
  } else {
    flags = O_CREAT | O_RDWR | O_NOFOLLOW;
    path = get_fname();
  }
  ret = openat(b->get_dir_fd(dpp), path.c_str(), flags, S_IRWXU);
  if (ret < 0) {
    ret = errno;
    ldpp_dout(dpp, 0) << "ERROR: could not open object " << get_name() << ": "
                  << cpp_strerror(ret) << dendl;
    return -ret;
  }

  obj_fd = ret;

  return 0;
}

int POSIXObject::link_temp_file(const DoutPrefixProvider *dpp)
{
  if (obj_fd < 0) {
    return 0;
  }

  char temp_file_path[PATH_MAX];
  // Only works on Linux - Non-portable
  snprintf(temp_file_path, PATH_MAX,  "/proc/self/fd/%d", obj_fd);

  POSIXBucket *b = dynamic_cast<POSIXBucket*>(get_bucket());

  if (!b) {
      ldpp_dout(dpp, 0) << "ERROR: could not get bucket for " << get_name() << dendl;
      return -EINVAL;
  }

  int ret = linkat(AT_FDCWD, temp_file_path, b->get_dir_fd(dpp), get_temp_fname().c_str(), AT_SYMLINK_FOLLOW);
  if(ret < 0) {
    ret = errno;
    ldpp_dout(dpp, 0) << "ERROR: linkat for temp file could not finish: "
	<< cpp_strerror(ret) << dendl;
    return -ret;
  }

  ret = renameat(b->get_dir_fd(dpp), get_temp_fname().c_str(), b->get_dir_fd(dpp), get_fname().c_str());
  if(ret < 0) {
    ret = errno;
    ldpp_dout(dpp, 0) << "ERROR: renameat for object could not finish: "
	<< cpp_strerror(ret) << dendl;
    return -ret;
  }


  return 0;
}


int POSIXObject::close()
{
  if (obj_fd < 0) {
    return 0;
  }

  int ret = ::fsync(obj_fd);
  if(ret < 0) {
    return ret;
  }

  ret = ::close(obj_fd);
  if(ret < 0) {
    return ret;
  }
  obj_fd = -1;

  return 0;
}

int POSIXObject::read(int64_t ofs, int64_t left, bufferlist& bl,
		      const DoutPrefixProvider* dpp, optional_yield y)
{
  if (!shadow) {
    // Normal file, just read it
    int64_t len = std::min(left + 1, READ_SIZE);
    ssize_t ret;

    ret = lseek(obj_fd, ofs, SEEK_SET);
    if (ret < 0) {
      ret = errno;
      ldpp_dout(dpp, 0) << "ERROR: could not seek object " << get_name() << " to "
	<< ofs << " :" << cpp_strerror(ret) << dendl;
      return -ret;
    }

    ret = ::read(obj_fd, read_buf, len);
    if (ret < 0) {
      ret = errno;
      ldpp_dout(dpp, 0) << "ERROR: could not read object " << get_name() << ": "
	<< cpp_strerror(ret) << dendl;
      return -ret;
    }

    bl.append(read_buf, ret);

    return ret;
  }

  // It's a multipart object, find the correct file, open it, and read it
  std::string pname;
  for (auto part : parts) {
    if (ofs < part.second) {
      pname = part.first;
      break;
    }

    ofs -= part.second;
  }

  if (pname.empty()) {
    // ofs is past the end
    return 0;
  }

  POSIXObject* shadow_obj;
  std::unique_ptr<rgw::sal::Object> obj = shadow->get_object(rgw_obj_key(pname));
  shadow_obj = static_cast<POSIXObject*>(obj.get());
  int ret = shadow_obj->open(dpp);
  if (ret < 0) {
    return ret;
  }

  return shadow_obj->read(ofs, left, bl, dpp, y);
}

int POSIXObject::write(int64_t ofs, bufferlist& bl, const DoutPrefixProvider* dpp,
		       optional_yield y)
{
  if (shadow) {
    // Can't write to a MP file
    return -EINVAL;
  }

  int64_t left = bl.length();
  char* curp = bl.c_str();
  ssize_t ret;

  ret = fchmod(obj_fd, S_IRUSR|S_IWUSR);
  if(ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: could not change permissions on object " << get_name() << ": "
                  << cpp_strerror(ret) << dendl;
    return ret;
  }


  ret = lseek(obj_fd, ofs, SEEK_SET);
  if (ret < 0) {
    ret = errno;
    ldpp_dout(dpp, 0) << "ERROR: could not seek object " << get_name() << " to "
      << ofs << " :" << cpp_strerror(ret) << dendl;
    return -ret;
  }

  while (left > 0) {
    ret = ::write(obj_fd, curp, left);
    if (ret < 0) {
      ret = errno;
      ldpp_dout(dpp, 0) << "ERROR: could not write object " << get_name() << ": "
	<< cpp_strerror(ret) << dendl;
      return -ret;
    }

    curp += ret;
    left -= ret;
  }

  return 0;
}

int POSIXObject::write_attr(const DoutPrefixProvider* dpp, optional_yield y, const std::string& key, bufferlist& value)
{
  int ret;
  std::string attrname;

  ret = open(dpp);
  if (ret < 0) {
    return ret;
  }

  return write_x_attr(dpp, y, obj_fd, key, value, get_name());
}

int POSIXObject::POSIXReadOp::prepare(optional_yield y, const DoutPrefixProvider* dpp)
{
  int ret = source->stat(dpp);
  if (ret < 0)
    return ret;

  ret = source->get_obj_attrs(y, dpp);
  if (ret < 0)
    return ret;

  auto iter = source->get_attrs().find(RGW_ATTR_ETAG);
  if (iter == source->get_attrs().end()) {
    /* Sideloaded file.  Generate necessary attributes. Only done once. */
    int ret = source->generate_attrs(dpp, y);
    if (ret < 0) {
	ldpp_dout(dpp, 0) << " ERROR: could not generate attrs for " << source->get_name() << " error: " << cpp_strerror(ret) << dendl;
	return ret;
    }
  }

  return 0;
}

int POSIXObject::POSIXReadOp::read(int64_t ofs, int64_t end, bufferlist& bl,
				     optional_yield y, const DoutPrefixProvider* dpp)
{
  return source->read(ofs, end + 1, bl, dpp, y);
}

int POSIXObject::generate_attrs(const DoutPrefixProvider* dpp, optional_yield y)
{
  int ret;

  /* Generate an ETAG */
  if (shadow) {
    ret = generate_mp_etag(dpp, y);
  } else {
    ret = generate_etag(dpp, y);
  }

  return ret;
}

int POSIXObject::generate_mp_etag(const DoutPrefixProvider* dpp, optional_yield y)
{
  int64_t count = 0;
  char etag_buf[CEPH_CRYPTO_MD5_DIGESTSIZE];
  char final_etag_str[CEPH_CRYPTO_MD5_DIGESTSIZE * 2 + 16];
  std::string etag;
  bufferlist etag_bl;
  MD5 hash;
  // Allow use of MD5 digest in FIPS mode for non-cryptographic purposes
  hash.SetFlags(EVP_MD_CTX_FLAG_NON_FIPS_ALLOW);
  int ret;
  rgw::sal::Bucket::ListParams params;
  rgw::sal::Bucket::ListResults results;

  do {
    static constexpr auto MAX_LIST_OBJS = 100u;
    ret = shadow->list(dpp, params, MAX_LIST_OBJS, results, null_yield);
    if (ret < 0) {
      return ret;
    }
    for (rgw_bucket_dir_entry& ent : results.objs) {
      std::unique_ptr<rgw::sal::Object> obj;
      POSIXObject* shadow_obj;

      if (MP_OBJ_PART_PFX.compare(0, std::string::npos, ent.key.name,
				  MP_OBJ_PART_PFX.size() != 0)) {
	// Skip non-parts
	continue;
      }

      rgw_obj_key key(ent.key);
      obj = shadow->get_object(rgw_obj_key(ent.key));
      shadow_obj = static_cast<POSIXObject*>(obj.get());
      ret = shadow_obj->get_obj_attrs(y, dpp);
      if (ret < 0) {
	return ret;
      }
      auto iter = shadow_obj->get_attrs().find(RGW_ATTR_ETAG);
      if (iter == shadow_obj->get_attrs().end()) {
	// Generate part's etag
	ret = shadow_obj->generate_etag(dpp, y);
	if (ret < 0)
	  return ret;
      }
      iter = shadow_obj->get_attrs().find(RGW_ATTR_ETAG);
      if (iter == shadow_obj->get_attrs().end()) {
	// Can't get etag.
	return -EINVAL;
      }
      hex_to_buf(iter->second.c_str(), etag_buf, CEPH_CRYPTO_MD5_DIGESTSIZE);
      hash.Update((const unsigned char *)etag_buf, sizeof(etag_buf));
      count++;
    }
  } while (results.is_truncated);

  hash.Final((unsigned char *)etag_buf);

  buf_to_hex((unsigned char *)etag_buf, sizeof(etag_buf), final_etag_str);
  snprintf(&final_etag_str[CEPH_CRYPTO_MD5_DIGESTSIZE * 2],
	   sizeof(final_etag_str) - CEPH_CRYPTO_MD5_DIGESTSIZE * 2,
           "-%lld", (long long)count);
  etag = final_etag_str;
  ldpp_dout(dpp, 10) << "calculated etag: " << etag << dendl;

  etag_bl.append(etag);
  (void)write_attr(dpp, y, RGW_ATTR_ETAG, etag_bl);
  get_attrs().emplace(std::move(RGW_ATTR_ETAG), std::move(etag_bl));

  return 0;
}

int POSIXObject::generate_etag(const DoutPrefixProvider* dpp, optional_yield y)
{
  int64_t left = get_obj_size();
  int64_t cur_ofs = 0;
  MD5 hash;
  // Allow use of MD5 digest in FIPS mode for non-cryptographic purposes
  hash.SetFlags(EVP_MD_CTX_FLAG_NON_FIPS_ALLOW);
  char calc_md5[CEPH_CRYPTO_MD5_DIGESTSIZE * 2 + 1];
  unsigned char m[CEPH_CRYPTO_MD5_DIGESTSIZE];

  bufferlist etag_bl;

  while (left > 0) {
    bufferlist bl;
    int len = read(cur_ofs, left, bl, dpp, y);
    if (len < 0) {
	ldpp_dout(dpp, 0) << " ERROR: could not read " << get_name() <<
	  " ofs: " << cur_ofs << " error: " << cpp_strerror(len) << dendl;
	return len;
    } else if (len == 0) {
      /* Done */
      break;
    }
    hash.Update((const unsigned char *)bl.c_str(), bl.length());

    left -= len;
    cur_ofs += len;
  }

  hash.Final(m);
  buf_to_hex(m, CEPH_CRYPTO_MD5_DIGESTSIZE, calc_md5);
  etag_bl.append(calc_md5, sizeof(calc_md5));
  (void)write_attr(dpp, y, RGW_ATTR_ETAG, etag_bl);
  get_attrs().emplace(std::move(RGW_ATTR_ETAG), std::move(etag_bl));

  return 0;
}

const std::string POSIXObject::get_fname()
{
  std::string fname = get_obj().get_oid();

  if (!get_obj().key.get_ns().empty()) {
    /* Namespaced objects are hidden */
    fname.insert(0, 1, '.');
  }

  return fname;
}

void POSIXObject::gen_temp_fname()
{
  enum { RAND_SUFFIX_SIZE = 8 };
  char buf[RAND_SUFFIX_SIZE + 1];

  gen_rand_alphanumeric_no_underscore(driver->ctx(), buf, RAND_SUFFIX_SIZE);
  temp_fname = "." + get_fname() + ".";
  temp_fname.append(buf);
}

const std::string POSIXObject::get_temp_fname()
{
  return temp_fname;
}

int POSIXObject::POSIXReadOp::iterate(const DoutPrefixProvider* dpp, int64_t ofs,
					int64_t end, RGWGetDataCB* cb, optional_yield y)
{
  int64_t left;
  int64_t cur_ofs = ofs;

  if (end < 0)
    left = 0;
  else
    left = end - ofs + 1;

  while (left > 0) {
    bufferlist bl;
    int len = source->read(cur_ofs, left, bl, dpp, y);
    if (len < 0) {
	ldpp_dout(dpp, 0) << " ERROR: could not read " << source->get_name() <<
	  " ofs: " << cur_ofs << " error: " << cpp_strerror(len) << dendl;
	return len;
    } else if (len == 0) {
      /* Done */
      break;
    }

    /* Read some */
    int ret = cb->handle_data(bl, 0, len);
    if (ret < 0) {
	ldpp_dout(dpp, 0) << " ERROR: callback failed on " << source->get_name() << dendl;
	return ret;
    }

    left -= len;
    cur_ofs += len;
  }

  /* Doesn't seem to be anything needed from params */
  return 0;
}

int POSIXObject::POSIXReadOp::get_attr(const DoutPrefixProvider* dpp, const char* name, bufferlist& dest, optional_yield y)
{
  auto& attrs = source->get_attrs();
  auto iter = attrs.find(name);
  if (iter == attrs.end()) {
    return -ENODATA;
  }

  dest = iter->second;
  return 0;
}

int POSIXObject::POSIXDeleteOp::delete_obj(const DoutPrefixProvider* dpp,
					   optional_yield y)
{
  return source->delete_object(dpp, y, false);
}

void POSIXMPObj::init_gen(POSIXDriver* driver, const std::string& _oid, ACLOwner& _owner)
{
  char buf[33];
  std::string new_id = MULTIPART_UPLOAD_ID_PREFIX; /* v2 upload id */
  /* Generate an upload ID */

  gen_rand_alphanumeric(driver->ctx(), buf, sizeof(buf) - 1);
  new_id.append(buf);
  init(_oid, new_id, _owner);
}

int POSIXMultipartPart::load(const DoutPrefixProvider* dpp, optional_yield y,
			     POSIXDriver* driver, rgw_obj_key& key)
{
  if (shadow) {
    /* Already loaded */
    return 0;
  }

  shadow = std::make_unique<POSIXObject>(driver, key, upload->get_shadow());

  RGWObjState* pstate;
  // Stat the shadow object to get things like size
  int ret = shadow->get_obj_state(dpp, &pstate, y);
  if (ret < 0) {
    return ret;
  }

  ret = shadow->get_obj_attrs(y, dpp);
  if (ret < 0) {
    return ret;
  }

  auto ait = shadow->get_attrs().find(RGW_POSIX_ATTR_MPUPLOAD);
  if (ait == shadow->get_attrs().end()) {
    ldout(driver->ctx(), 0) << "ERROR: " << __func__ << ": Not a part: " << key << dendl;
    return -EINVAL;
  }

  try {
    auto bit = ait->second.cbegin();
    decode(info, bit);
  } catch (buffer::error& err) {
    ldout(driver->ctx(), 0) << "ERROR: " << __func__ << ": failed to decode part info: " << key << dendl;
    return -EINVAL;
  }

  return 0;
}

int POSIXMultipartUpload::load(bool create)
{
  ldout(driver->ctx(), 0) << "Luke: load shadow " << get_meta() << dendl;
  if (!shadow) {
    POSIXBucket* pb = dynamic_cast<POSIXBucket*>(bucket);
    return pb->get_shadow_bucket(nullptr, null_yield, mp_ns,
			  std::string(), get_meta(), create, &shadow);
  }

  return 0;
}

std::unique_ptr<rgw::sal::Object> POSIXMultipartUpload::get_meta_obj()
{
  load();
  return shadow->get_object(rgw_obj_key(get_meta(), std::string(), mp_ns));
}

int POSIXMultipartUpload::init(const DoutPrefixProvider *dpp, optional_yield y,
				ACLOwner& owner, rgw_placement_rule& dest_placement,
				rgw::sal::Attrs& attrs)
{
  int ret;

  /* Create the shadow bucket */
  ret = load(true);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << " ERROR: could not get shadow bucket for mp upload "
      << get_key() << dendl;
    return ret;
  }

  /* Now create the meta object */
  std::unique_ptr<rgw::sal::Object> meta_obj;

  meta_obj = get_meta_obj();

  mp_obj.upload_info.dest_placement = dest_placement;

  bufferlist bl;
  encode(mp_obj, bl);

  attrs[RGW_POSIX_ATTR_MPUPLOAD] = bl;

  return meta_obj->set_obj_attrs(dpp, &attrs, nullptr, y);
}

int POSIXMultipartUpload::list_parts(const DoutPrefixProvider *dpp, CephContext *cct,
				      int num_parts, int marker,
				      int *next_marker, bool *truncated,
				      bool assume_unsorted)
{
  int ret;
  int last_num = 0;

  ret = load();
  if (ret < 0) {
    return ret;
  }

  rgw::sal::Bucket::ListParams params;
  rgw::sal::Bucket::ListResults results;

  params.prefix = MP_OBJ_PART_PFX;
  params.marker = MP_OBJ_PART_PFX + fmt::format("{:0>5}", marker);

  ret = shadow->list(dpp, params, num_parts + 1, results, null_yield);
  if (ret < 0) {
    return ret;
  }
  for (rgw_bucket_dir_entry& ent : results.objs) {
    std::unique_ptr<MultipartPart> part = std::make_unique<POSIXMultipartPart>(this);
    POSIXMultipartPart* ppart = dynamic_cast<POSIXMultipartPart*>(part.get());

    rgw_obj_key key(ent.key);
    ret = ppart->load(dpp, null_yield, driver, key);
    if (ret == 0) {
      /* Skip anything that's not a part */
      last_num = part->get_num();
      parts[part->get_num()] = std::move(part);
    }
    if (parts.size() == (ulong)num_parts)
      break;
  }

  if (truncated)
    *truncated = results.is_truncated;

  if (next_marker)
    *next_marker = last_num;

  return 0;
}

int POSIXMultipartUpload::abort(const DoutPrefixProvider *dpp, CephContext *cct)
{
  int ret;

  ret = load();
  if (ret < 0) {
    return ret;
  }

  shadow->remove_bucket(dpp, true, false, nullptr, null_yield);

  return 0;
}

int POSIXMultipartUpload::complete(const DoutPrefixProvider *dpp,
				    optional_yield y, CephContext* cct,
				    std::map<int, std::string>& part_etags,
				    std::list<rgw_obj_index_key>& remove_objs,
				    uint64_t& accounted_size, bool& compressed,
				    RGWCompressionInfo& cs_info, off_t& ofs,
				    std::string& tag, ACLOwner& owner,
				    uint64_t olh_epoch,
				    rgw::sal::Object* target_obj)
{
  char final_etag[CEPH_CRYPTO_MD5_DIGESTSIZE];
  char final_etag_str[CEPH_CRYPTO_MD5_DIGESTSIZE * 2 + 16];
  std::string etag;
  bufferlist etag_bl;
  MD5 hash;
  // Allow use of MD5 digest in FIPS mode for non-cryptographic purposes
  hash.SetFlags(EVP_MD_CTX_FLAG_NON_FIPS_ALLOW);
  bool truncated;
  int ret;

  int total_parts = 0;
  int handled_parts = 0;
  int max_parts = 1000;
  int marker = 0;
  uint64_t min_part_size = cct->_conf->rgw_multipart_min_part_size;
  auto etags_iter = part_etags.begin();
  rgw::sal::Attrs attrs = target_obj->get_attrs();

  do {
    ret = list_parts(dpp, cct, max_parts, marker, &marker, &truncated);
    if (ret == -ENOENT) {
      ret = -ERR_NO_SUCH_UPLOAD;
    }
    if (ret < 0)
      return ret;

    total_parts += parts.size();
    if (!truncated && total_parts != (int)part_etags.size()) {
      ldpp_dout(dpp, 0) << "NOTICE: total parts mismatch: have: " << total_parts
		       << " expected: " << part_etags.size() << dendl;
      ret = -ERR_INVALID_PART;
      return ret;
    }

    for (auto obj_iter = parts.begin(); etags_iter != part_etags.end() && obj_iter != parts.end(); ++etags_iter, ++obj_iter, ++handled_parts) {
      POSIXMultipartPart* part = dynamic_cast<rgw::sal::POSIXMultipartPart*>(obj_iter->second.get());
      uint64_t part_size = part->get_size();
      if (handled_parts < (int)part_etags.size() - 1 &&
          part_size < min_part_size) {
        ret = -ERR_TOO_SMALL;
        return ret;
      }

      char petag[CEPH_CRYPTO_MD5_DIGESTSIZE];
      if (etags_iter->first != (int)obj_iter->first) {
        ldpp_dout(dpp, 0) << "NOTICE: parts num mismatch: next requested: "
			 << etags_iter->first << " next uploaded: "
			 << obj_iter->first << dendl;
        ret = -ERR_INVALID_PART;
        return ret;
      }
      std::string part_etag = rgw_string_unquote(etags_iter->second);
      if (part_etag.compare(part->get_etag()) != 0) {
        ldpp_dout(dpp, 0) << "NOTICE: etag mismatch: part: " << etags_iter->first
			 << " etag: " << etags_iter->second << dendl;
        ret = -ERR_INVALID_PART;
        return ret;
      }

      hex_to_buf(part->get_etag().c_str(), petag,
		CEPH_CRYPTO_MD5_DIGESTSIZE);
      hash.Update((const unsigned char *)petag, sizeof(petag));

      // Compression is not supported yet
#if 0
      RGWUploadPartInfo& obj_part = part->info;

      bool part_compressed = (obj_part.cs_info.compression_type != "none");
      if ((handled_parts > 0) &&
          ((part_compressed != compressed) ||
            (cs_info.compression_type != obj_part.cs_info.compression_type))) {
          ldpp_dout(dpp, 0) << "ERROR: compression type was changed during multipart upload ("
                           << cs_info.compression_type << ">>" << obj_part.cs_info.compression_type << ")" << dendl;
          ret = -ERR_INVALID_PART;
          return ret;
      }

      if (part_compressed) {
        int64_t new_ofs; // offset in compression data for new part
        if (cs_info.blocks.size() > 0)
          new_ofs = cs_info.blocks.back().new_ofs + cs_info.blocks.back().len;
        else
          new_ofs = 0;
        for (const auto& block : obj_part.cs_info.blocks) {
          compression_block cb;
          cb.old_ofs = block.old_ofs + cs_info.orig_size;
          cb.new_ofs = new_ofs;
          cb.len = block.len;
          cs_info.blocks.push_back(cb);
          new_ofs = cb.new_ofs + cb.len;
        }
        if (!compressed)
          cs_info.compression_type = obj_part.cs_info.compression_type;
        cs_info.orig_size += obj_part.cs_info.orig_size;
        compressed = true;
      }
#endif

      ofs += part->get_size();
      accounted_size += part->get_size();
    }
  } while (truncated);
  hash.Final((unsigned char *)final_etag);

  buf_to_hex((unsigned char *)final_etag, sizeof(final_etag), final_etag_str);
  snprintf(&final_etag_str[CEPH_CRYPTO_MD5_DIGESTSIZE * 2],
	   sizeof(final_etag_str) - CEPH_CRYPTO_MD5_DIGESTSIZE * 2,
           "-%lld", (long long)part_etags.size());
  etag = final_etag_str;
  ldpp_dout(dpp, 10) << "calculated etag: " << etag << dendl;

  etag_bl.append(etag);

  attrs[RGW_ATTR_ETAG] = etag_bl;

  if (compressed) {
    // write compression attribute to full object
    bufferlist tmp;
    encode(cs_info, tmp);
    attrs[RGW_ATTR_COMPRESSION] = tmp;
  }

  // Rename to target_obj
  return shadow->rename(dpp, y, target_obj);
}

int POSIXMultipartUpload::get_info(const DoutPrefixProvider *dpp, optional_yield y,
				   rgw_placement_rule** rule, rgw::sal::Attrs* attrs)
{
  std::unique_ptr<rgw::sal::Object> meta_obj;
  int ret;

  if (!rule && !attrs) {
    return 0;
  }

  if (attrs) {
      meta_obj = get_meta_obj();
      int ret = meta_obj->get_obj_attrs(y, dpp);
      if (ret < 0) {
	ldpp_dout(dpp, 0) << " ERROR: could not get meta object for mp upload "
	  << get_key() << dendl;
	return ret;
      }
      *attrs = meta_obj->get_attrs();
  }

  if (rule) {
    if (mp_obj.oid.empty()) {
      if (!meta_obj) {
	meta_obj = get_meta_obj();
	ret = meta_obj->get_obj_attrs(y, dpp);
	if (ret < 0) {
	  ldpp_dout(dpp, 0) << " ERROR: could not get meta object for mp upload "
	    << get_key() << dendl;
	  return ret;
	}
      }
      auto iter = meta_obj->get_attrs().find(RGW_POSIX_ATTR_MPUPLOAD);
      if (iter == meta_obj->get_attrs().end()) {
	ldpp_dout(dpp, 0) << " ERROR: could not get meta object attrs for mp upload "
	  << get_key() << dendl;
	return ret;
      }
      auto biter = iter->second.cbegin();
      decode(mp_obj, biter);
    }
    *rule = &mp_obj.upload_info.dest_placement;
  }

  return 0;
}

std::unique_ptr<Writer> POSIXMultipartUpload::get_writer(
				  const DoutPrefixProvider *dpp,
				  optional_yield y,
				  std::unique_ptr<rgw::sal::Object> _head_obj,
				  const rgw_user& owner,
				  const rgw_placement_rule *ptail_placement_rule,
				  uint64_t part_num,
				  const std::string& part_num_str)
{
  std::string fname = MP_OBJ_PART_PFX + fmt::format("{:0>5}", part_num);
  rgw_obj_key part_key(fname);

  load();

  return std::make_unique<POSIXMultipartWriter>(dpp, y, shadow->clone(), part_key, driver,
						owner, ptail_placement_rule, part_num);
}

int POSIXMultipartWriter::prepare(optional_yield y)
{
  return obj->open(dpp);
}

int POSIXMultipartWriter::process(bufferlist&& data, uint64_t offset)
{
  return obj->write(offset, data, dpp, null_yield);
}

int POSIXMultipartWriter::complete(size_t accounted_size, const std::string& etag,
                       ceph::real_time *mtime, ceph::real_time set_mtime,
                       std::map<std::string, bufferlist>& attrs,
                       ceph::real_time delete_at,
                       const char *if_match, const char *if_nomatch,
                       const std::string *user_data,
                       rgw_zone_set *zones_trace, bool *canceled,
                       optional_yield y)
{
  int ret;
  POSIXUploadPartInfo info;
  bufferlist bl;

  info.num = part_num;
  info.etag = etag;
  info.mtime = set_mtime;

  encode(info, bl);
  attrs[RGW_POSIX_ATTR_MPUPLOAD] = bl;

  for (auto attr : attrs) {
    ret = obj->write_attr(dpp, y, attr.first, attr.second);
    if (ret < 0) {
      ldpp_dout(dpp, 20) << "ERROR: failed writing attr " << attr.first << dendl;
      return ret;
    }
  }

  ret = obj->close();
  if (ret < 0) {
    ldpp_dout(dpp, 20) << "ERROR: failed closing file" << dendl;
    return ret;
  }

  return 0;
}

int POSIXWriter::prepare(optional_yield y)
{
  return next->prepare(y);
}

int POSIXWriter::process(bufferlist&& data, uint64_t offset)
{
  return next->process(std::move(data), offset);
}

int POSIXWriter::complete(size_t accounted_size, const std::string& etag,
                       ceph::real_time *mtime, ceph::real_time set_mtime,
                       std::map<std::string, bufferlist>& attrs,
                       ceph::real_time delete_at,
                       const char *if_match, const char *if_nomatch,
                       const std::string *user_data,
                       rgw_zone_set *zones_trace, bool *canceled,
                       optional_yield y)
{
  return next->complete(accounted_size, etag, mtime, set_mtime, attrs,
			delete_at, if_match, if_nomatch, user_data, zones_trace,
			canceled, y);
}


int POSIXAtomicWriter::prepare(optional_yield y)
{
  obj.gen_temp_fname();
  return obj.open(dpp, true);
}

int POSIXAtomicWriter::process(bufferlist&& data, uint64_t offset)
{
  return obj.write(offset, data, dpp, null_yield);
}

int POSIXAtomicWriter::complete(size_t accounted_size, const std::string& etag,
                       ceph::real_time *mtime, ceph::real_time set_mtime,
                       std::map<std::string, bufferlist>& attrs,
                       ceph::real_time delete_at,
                       const char *if_match, const char *if_nomatch,
                       const std::string *user_data,
                       rgw_zone_set *zones_trace, bool *canceled,
                       optional_yield y)
{
  int ret;
  for (auto attr : attrs) {
    ret = obj.write_attr(dpp, y, attr.first, attr.second);
    if (ret < 0) {
      ldpp_dout(dpp, 20) << "ERROR: POSIXAtomicWriter failed writing attr " << attr.first << dendl;
      return ret;
    }
  }

  ret = obj.link_temp_file(dpp);
  if (ret < 0) {
    ldpp_dout(dpp, 20) << "ERROR: POSIXAtomicWriter failed writing temp file" << dendl;
    return ret;
  }

  ret = obj.close();
  if (ret < 0) {
    ldpp_dout(dpp, 20) << "ERROR: POSIXAtomicWriter failed closing file" << dendl;
    return ret;
  }

  return 0;
}

} } // namespace rgw::sal

extern "C" {

rgw::sal::Driver* newPOSIXDriver(rgw::sal::Driver* next)
{
  rgw::sal::POSIXDriver* driver = new rgw::sal::POSIXDriver(next);

  return driver;
}

}