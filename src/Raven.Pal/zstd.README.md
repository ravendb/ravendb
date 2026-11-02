# RavenDB.Zstd.Native

Native [Zstandard (zstd)](https://github.com/facebook/zstd) library used by [RavenDB](https://ravendb.net) - the RavenDB client (`RavenDB.Client`) and the RavenDB server - for zstd compression.

The package contains no .NET code, only `libzstd` built from the upstream zstd release (shared library, multi-threaded), placed where the .NET host finds it:

| Runtime | File |
|---|---|
| win-x86, win-x64, win-arm64 | `runtimes/<rid>/native/libzstd.dll` |
| linux-x64, linux-arm, linux-arm64 | `runtimes/<rid>/native/libzstd.so` |
| osx-arm64 | `runtimes/<rid>/native/libzstd.dylib` |

You don't need to reference this package yourself: `RavenDB.Client` depends on it.

## License

zstd is dual-licensed under BSD-3-Clause or GPL-2.0-only; the BSD license text is included as `LICENSE.txt`.
Copyright (c) Meta Platforms, Inc. and affiliates.
