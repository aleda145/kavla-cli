#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 5 ]]; then
  echo "usage: $0 <version> <goos> <goarch> <input-dir> <dist-dir>" >&2
  exit 1
fi

version="$1"
goos="$2"
goarch="$3"
input_dir="$4"
dist_dir="$5"
archive_name="kavla_${version}_${goos}_${goarch}.tar.gz"

if [[ ! -f "$input_dir/kavla" ]]; then
  echo "expected $input_dir/kavla to exist" >&2
  exit 1
fi

mkdir -p "$dist_dir"
rm -f "$dist_dir/$archive_name"
tar -C "$input_dir" -czf "$dist_dir/$archive_name" kavla
