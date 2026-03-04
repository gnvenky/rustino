#!/usr/bin/env zsh
set -euo pipefail

# Find candidate files (matches registry path)
FILES=(~/.cargo/registry/src/index.crates.io-*/arrow-arith-50.0.0/src/temporal.rs)

if (( ${#FILES[@]} == 0 )); then
  echo "No matching files found under ~/.cargo/registry. Exiting."
  exit 1
fi

for f in "${FILES[@]}"; do
  if [[ ! -f "$f" ]]; then
    echo "Not a file: $f"
    continue
  fi

  echo "Backing up: $f"
  bak="${f}.orig.$(date +%s)"
  cp -- "$f" "$bak"

  echo "Patching: $f"
  perl -0777 -i -pe '
    s/time_fraction_dyn\(array, "quarter", \|t\| t\.quarter\(\) as i32\)/time_fraction_dyn(array, "quarter", |t| ChronoDateExt::quarter(&t) as i32)/g;
    s/time_fraction_internal\(array, "quarter", \|t\| t\.quarter\(\) as i32\)/time_fraction_internal(array, "quarter", |t| ChronoDateExt::quarter(&t) as i32)/g;
  ' "$f"

  echo "Patched and backed up to: $bak"
done

echo "Done. To rebuild the project run:"
echo "  cargo clean && cargo build"
