/*
 * Copyright (C) 2019 Yang Yi <yyrust@gmail.com>
 * All rights reserved.
 *
 * This software is licensed as described in the file LICENSE, which
 * you should have received as part of this distribution.
 *
 * Author: Yang Yi <yyrust@gmail.com>
 */
#include <cstddef>
#include <string>
#include <vector>
#include <cstdio>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <cstring>
#include <cerrno>
#include <dirent.h>
#include <sys/time.h>
#include <rapidjson/writer.h>
#include <rapidjson/prettywriter.h>
#include <rapidjson/filewritestream.h>
#include <rapidjson/filereadstream.h>
#include <rapidjson/error/en.h>
#include <rapidjson/document.h>
#include <algorithm>

using namespace rapidjson;
typedef GenericDocument<UTF8<> > JDocument;
typedef GenericValue<UTF8<> > JValue;

enum FileType {
    FILE_TYPE_UNKNOWN = 0,
    FILE_TYPE_REGULAR = 1,
    FILE_TYPE_DIRECTORY = 2,
    FILE_TYPE_LINK = 3,
};

const size_t BLOCK_SIZE = 512;
const char SEP = '/';

#define LOG(FMT, ARGS...)\
    fprintf(stderr, "%s:%d: " FMT "\n", __FILE__, __LINE__, ARGS);

std::string join_path(const std::string &prefix, const std::string &postfix)
{
    bool hasSep = (!prefix.empty() && prefix[prefix.size() - 1] == SEP);
    if (hasSep)
        return prefix + postfix;
    else
        return prefix + SEP + postfix;
}

std::string flatten_path(const std::string &path)
{
    std::vector<char> buf;
    buf.resize(path.size());
    for (size_t i = 0; i < path.size(); i++) {
        int c = path[i];
        buf[i] = (c == SEP) ? '_' : c;
    }
    return std::string(buf.data(), buf.size());
}

std::string time_to_str()
{
    struct timeval tv;
    gettimeofday(&tv, NULL);
    char buf[128];
    struct tm &tm = *localtime(&tv.tv_sec);
    snprintf(buf, 128, "%04d.%02d.%02d-%02d.%02d.%02d.%06zu",
            tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday,
            tm.tm_hour, tm.tm_min, tm.tm_sec,
            (size_t)tv.tv_usec);
    return buf;
}

std::string make_json_file_name(const std::string &path)
{
    return "dirs_" + flatten_path(path) + time_to_str() + ".json";
}

std::string readable_size(size_t nbytes)
{
    const size_t nlevels = 4;

    struct LevelInfo {
        size_t size;
        char unit;
    } levels[nlevels] = {
        {size_t(1024) * 1024 * 1024 * 1024, 'T'},
        {1024 * 1024 * 1024, 'G'},
        {1024 * 1024, 'M'},
        {1024, 'K'},
    };

    for (size_t i = 0; i < nlevels; i++) {
        size_t size = levels[i].size;
        char unit = levels[i].unit;
        if (nbytes >= size) {
            size_t low_size = size / 1024;
            size_t quot = nbytes / size;
            size_t remainder = (nbytes - quot * size) / low_size;
            char buf[64];
            if (remainder == 0) {
                snprintf(buf, 64, "%zu%c", quot, unit);
            }
            else {
                double fquot = quot + remainder / 1024.0;
                snprintf(buf, 64, "%.03lf%c", fquot, unit);
            }
            return buf;
        }
    }
    char buf[64];
    snprintf(buf, 64, "%zu", nbytes);
    return buf;
}

// non-reentrant version, for convenience
const char *r_sz(size_t nbytes)
{
    static std::string s;
    s = readable_size(nbytes);
    return s.c_str();
}

struct FileInfo
{
    size_t size_;
    std::string path_;
    std::vector<FileInfo *> sub_files_;
    int type_;

    FileInfo()
    : size_(0)
    , type_(FILE_TYPE_UNKNOWN)
    {}

    ~FileInfo()
    {
        for (size_t i = 0; i < sub_files_.size(); i++) {
            delete sub_files_[i];
        }
    }

    void set_path(const std::string &path)
    {
        path_ = path;
        struct stat st;
        errno = 0;
        int ret = lstat(path.c_str(), &st);
        if (ret != 0) {
            LOG("lstat(%s) failed: %s", path.c_str(), strerror(errno));
            return;
        }
        if (S_ISDIR(st.st_mode)) {
            type_ = FILE_TYPE_DIRECTORY;
            size_ = st.st_blocks * BLOCK_SIZE;
        }
        else if (S_ISREG(st.st_mode)) {
            type_ = FILE_TYPE_REGULAR;
            size_ = st.st_blocks * BLOCK_SIZE;
        }
        else if (S_ISLNK(st.st_mode)) {
            type_ = FILE_TYPE_LINK;
            size_ = st.st_blocks * BLOCK_SIZE;
        }
        else {
            type_ = FILE_TYPE_UNKNOWN;
        }
    }

    void walk(int depth)
    {
        if (type_ != FILE_TYPE_DIRECTORY)
            return;

        DIR *dir;
        struct dirent *ent;
        if ((dir = opendir (path_.c_str())) != NULL) {
            while ((ent = readdir (dir)) != NULL) {
                if (0 == strcmp(".", ent->d_name) || 0 == strcmp("..", ent->d_name)) {
                    continue;
                }
                std::string path = join_path(path_, ent->d_name);
                FileInfo *sub_file = new FileInfo();
                sub_file->set_path(path);
                if (sub_file->type_ == FILE_TYPE_UNKNOWN) {
                    delete sub_file;
                    continue;
                }
                if (depth > 0) {
                    sub_files_.push_back(sub_file);
                }
                if (sub_file->type_ == FILE_TYPE_DIRECTORY) {
                    sub_file->walk(depth - 1);
                }
                size_ += sub_file->size_;
            }
            closedir (dir);
        } else {
            LOG("could not open dir %s: %s", path_.c_str(), strerror(errno));
        }
    }

    void diff(const FileInfo &older) const
    {
        if (size_ <= older.size_) {
            return;
        }

        bool both_dirs = (type_ == FILE_TYPE_DIRECTORY) && (older.type_ == FILE_TYPE_DIRECTORY);
        if (both_dirs) {
            size_t change_count = 0;
            size_t last_inc = 0;
            size_t i = 0, j = 0;
            while (i < sub_files_.size() && j < older.sub_files_.size()) {
                const FileInfo &lfile = *sub_files_[i];
                const FileInfo &rfile = *older.sub_files_[j];
                const std::string &lpath = lfile.path_;
                const std::string &rpath = rfile.path_;
                if (lpath == rpath) {
                    lfile.diff(rfile);
                    i++;
                    j++;
                    change_count += (lfile.size_ != rfile.size_) ? 1 : 0;
                    if (lfile.size_ > rfile.size_) {
                        last_inc = lfile.size_ - rfile.size_;
                    }
                }
                else if (lpath < rpath) {
                    i++;
                    LOG("%s\tnew +%s", lpath.c_str(), r_sz(lfile.size_));
                    if (lfile.size_ > 0) {
                        change_count++;
                        last_inc = lfile.size_;
                    }
                }
                else {
                    j++;
                    LOG("%s\tdel -%s", rpath.c_str(), r_sz(rfile.size_));
                    if (rfile.size_ > 0) {
                        change_count++;
                    }
                }
            }
            for (; i < sub_files_.size(); i++) {
                const FileInfo &lfile = *sub_files_[i];
                const std::string &lpath = lfile.path_;
                LOG("%s\tnew +%s", lpath.c_str(), r_sz(lfile.size_));
                last_inc = lfile.size_;
            }
            size_t total_inc = size_ - older.size_;
            if (change_count == 1 && last_inc == total_inc) {
                // the size change is caused by a sub file
            }
            else {
                LOG("%s\t+%s", path_.c_str(), r_sz(total_inc));
            }
        }
        else {
            LOG("%s\t+%s", path_.c_str(), r_sz(size_ - older.size_));
        }
        
    }

    template <typename Writer>
    void to_json(Writer &writer)
    {
        writer.StartObject();
        writer.Key("path");
        writer.String(path_.c_str(), path_.size());
        writer.Key("size");
        writer.Uint(size_);
        writer.Key("type");
        writer.Uint(type_);
        if (!sub_files_.empty()) {
            writer.Key("subs");
            writer.StartArray();
            for (size_t i = 0; i < sub_files_.size(); i++) {
                sub_files_[i]->to_json(writer);
            }
            writer.EndArray();
        }
        writer.EndObject();
    }

    bool from_json(const JValue &object)
    {
        if (!object.IsObject()) {
            LOG("json value is not an object, actual type id is %d", object.GetType());
            return false;
        }
        for (JValue::ConstMemberIterator it = object.MemberBegin(); it != object.MemberEnd(); ++it) {
            const JValue &value = it->value;
            const char *name = it->name.GetString();
            if (0 == strcmp("path", name)) {
                const char *s = value.GetString();
                size_t len = value.GetStringLength();
                path_.assign(s, len);
            }
            else if (0 == strcmp("size", name)) {
                size_ = value.GetUint();
            }
            else if (0 == strcmp("type", name)) {
                type_ = value.GetUint();
            }
            else if (0 == strcmp("subs", name)) {
                if (!value.IsArray()) {
                    LOG("WARN: json value for 'subs' is not an array, actual type id is %d", value.GetType());
                    continue;
                }
                for (JValue::ConstValueIterator it_sub = value.Begin(); it_sub != value.End(); ++it_sub) {
                    FileInfo *sub = new FileInfo();
                    if (!sub->from_json(*it_sub)) {
                        delete sub;
                    } else {
                        sub_files_.push_back(sub);
                    }
                }
                sort_subs();
            }
        }
        return true;
    }

    bool from_json(const std::string &file_path)
    {
        FILE *fp = fopen(file_path.c_str(), "r");
        if (!fp) {
            LOG("cannot open file %s", file_path.c_str());
            return false;
        }
        const size_t BUF_SIZE = 1024 * 256;
        char buf[BUF_SIZE];
        FileReadStream fs(fp, buf, BUF_SIZE);
        JDocument doc;
        doc.ParseStream(fs);
        fclose(fp);
        if (doc.HasParseError()) {
            LOG("failed to parse json from %s, error at %zu: %s",
                    file_path.c_str(),
                    doc.GetErrorOffset(),
                    GetParseError_En(doc.GetParseError())
                    );
            return false;
        }
        return from_json(doc);
    }

    struct PathLess {
        bool operator()(const FileInfo *lhs, const FileInfo *rhs)
        {
            return lhs->path_ < rhs->path_;
        }
    };
    void sort_subs()
    {
        std::sort(sub_files_.begin(), sub_files_.end(), PathLess());
    }
};

int cmd_stat(int argc, char *args[])
{
    if (argc != 1) {
        LOG("expect one argument: %s", "dir");
        return -1;
    }
    const char *root = args[0];
    FileInfo root_file;
    root_file.set_path(root);
    root_file.walk(5);

    const size_t BUF_SIZE = 1024 * 256;
    char buf[BUF_SIZE];
    std::string output_filename = make_json_file_name(root);
    FILE *fp = fopen(output_filename.c_str(), "wb");
    if (!fp) {
        LOG("cannot open file %s", "dirs.json");
        return -1;
    }
    FileWriteStream fs(fp, buf, BUF_SIZE);
    // Writer<FileWriteStream> writer(fs);
    PrettyWriter<FileWriteStream> writer(fs);
    writer.SetIndent(' ', 1);
    root_file.to_json(writer);
    fclose(fp);
    return 0;
}

int cmd_diff(int argc, char *args[])
{
    if (argc != 2) {
        LOG("expect two argument: %s %s", "old_stat.json", "new_stat.json");
        return -1;
    }

    FileInfo old_file, new_file;
    LOG("loading %s", args[0]);
    if (!old_file.from_json(args[0])) {
        LOG("failed to load %s", args[0]);
        return -1;
    }
    LOG("loading %s", args[1]);
    if (!new_file.from_json(args[1])) {
        LOG("failed to load %s", args[1]);
        return -1;
    }
    LOG("comparing %s %s", args[0], args[1]);
    new_file.diff(old_file);
    return 0;
}

int main(int argc, char *argv[])
{
    if (argc < 2) {
        printf(
                "Usage:\n"
                "    %s s[tat] dir\n"
                "    %s d[iff] old_stat.json new_stat.json\n",
                argv[0], argv[0]
                );
        return 0;
    }

    const char *cmd = argv[1];
    char **args = argv + 2;

    if (0 == strcmp("s", cmd) || 0 == strcmp("stat", cmd)) {
        return cmd_stat(argc - 2, args);
    }
    else if (0 == strcmp("d", cmd) || 0 == strcmp("diff", cmd)) {
        return cmd_diff(argc - 2, args);
    }
    else {
        LOG("invalid command %s", cmd);
        return -1;
    }

    return 0;
}
