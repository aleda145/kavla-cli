#!/usr/bin/env bash
set -euo pipefail

repo="${KAVLA_REPO:-aleda145/kavla-cli}"
install_dir="${KAVLA_INSTALL_DIR:-$HOME/.local/bin}"
version="${KAVLA_VERSION:-}"
required_tools=(curl tar awk)

fail() {
  echo "error: $*" >&2
  exit 1
}

require_tool() {
  local tool="$1"
  command -v "$tool" >/dev/null 2>&1 || fail "required tool not found: $tool"
}

detect_platform() {
  local uname_s uname_m
  uname_s="$(uname -s)"
  uname_m="$(uname -m)"

  case "$uname_s" in
    Linux)
      goos="linux"
      ;;
    Darwin)
      goos="darwin"
      ;;
    *)
      fail "unsupported operating system: $uname_s"
      ;;
  esac

  case "$uname_m" in
    x86_64|amd64)
      goarch="amd64"
      ;;
    arm64|aarch64)
      goarch="arm64"
      ;;
    *)
      fail "unsupported architecture: $uname_m"
      ;;
  esac
}

resolve_latest_version() {
  local latest_url
  latest_url="$(curl -fsSLI -o /dev/null -w '%{url_effective}' "https://github.com/${repo}/releases/latest")"

  [[ -n "$latest_url" ]] || fail "could not resolve the latest release URL"
  version="${latest_url##*/}"
  [[ "$version" =~ ^v[0-9]+(\.[0-9]+){2}([-.][0-9A-Za-z.-]+)?$ ]] || fail "resolved latest version is invalid: $version"
}

select_checksum_command() {
  if command -v sha256sum >/dev/null 2>&1; then
    checksum_cmd=(sha256sum -c)
    return
  fi

  if command -v shasum >/dev/null 2>&1; then
    checksum_cmd=(shasum -a 256 -c)
    return
  fi

  fail "required checksum tool not found: sha256sum or shasum"
}

verify_checksum() {
  local archive_name="$1"
  local checksums_file="$2"
  local filtered_checksums="${checksums_file}.filtered"

  (
    cd "$tmpdir"
    awk -v archive_name="$archive_name" '$2 == archive_name || $2 == "./" archive_name { print }' "$checksums_file" > "$filtered_checksums"
    [[ -s "$filtered_checksums" ]] || fail "checksum entry missing for ${archive_name}"
    "${checksum_cmd[@]}" "$filtered_checksums"
  )
}

print_path_hint() {
  case ":${PATH}:" in
    *":${install_dir}:"*)
      ;;
    *)
      echo
      echo "Add ${install_dir} to your PATH if it is not already available:"
      echo "  export PATH=\"${install_dir}:\$PATH\""
      ;;
  esac
}

for tool in "${required_tools[@]}"; do
  require_tool "$tool"
done

detect_platform
select_checksum_command

if [[ -z "$version" ]]; then
  resolve_latest_version
fi

archive_name="kavla_${version}_${goos}_${goarch}.tar.gz"
checksums_name="kavla_${version}_checksums.txt"
release_url="https://github.com/${repo}/releases/download/${version}"
tmpdir="$(mktemp -d)"
archive_path="${tmpdir}/${archive_name}"
checksums_path="${tmpdir}/${checksums_name}"
extract_dir="${tmpdir}/extract"

cleanup() {
  rm -rf "$tmpdir"
}
trap cleanup EXIT

echo "Installing Kavla CLI ${version} for ${goos}/${goarch}..."
curl -fsSL "${release_url}/${archive_name}" -o "$archive_path" || fail "failed to download ${archive_name}"
curl -fsSL "${release_url}/${checksums_name}" -o "$checksums_path" || fail "failed to download ${checksums_name}"

verify_checksum "$archive_name" "$checksums_name"

mkdir -p "$extract_dir"
tar -C "$extract_dir" -xzf "$archive_path" || fail "failed to extract ${archive_name}"
[[ -f "${extract_dir}/kavla" ]] || fail "archive did not contain a kavla binary"

mkdir -p "$install_dir" || fail "failed to create install directory: ${install_dir}"
install_path="${install_dir}/kavla"
cp "${extract_dir}/kavla" "$install_path" || fail "failed to install kavla to ${install_path}"
chmod 0755 "$install_path" || fail "failed to mark ${install_path} executable"

echo "Installed kavla to ${install_path}"
if ! version_output="$($install_path version 2>&1)"; then
  echo "$version_output" >&2
  fail "installed kavla could not be executed on this machine"
fi

echo "$version_output"
print_path_hint
