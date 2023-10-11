#!/usr/bin/env bash

ozone_snap_dir="hadoop-ozone/dist/target/ozone-1.4.0-SNAPSHOT"
libext_path="$ozone_snap_dir/share/ozone/lib/libext"

ranger_utils_dir="../../utils/ranger-files"

mkdir "$libext_path"

cp "$ranger_utils_dir"/conf/* "$ozone_snap_dir"/etc/hadoop/

ls -lah "$ozone_snap_dir"/etc/hadoop/

cp "$ranger_utils_dir"/ranger-ozone-plugin-3.0.0-SNAPSHOT.jar "$libext_path/"

ls -lah "$libext_path/"

