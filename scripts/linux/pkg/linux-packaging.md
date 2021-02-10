# RavenDB installation from package on Linux

## What the package does on install

- scatter the files in the default locations

- install systemd service

- add soft links to binaries

- add `ravendb` user and group

- set file permissions properly

- setcap on the Raven.Server

- print the server listen port

## Supported use cases

### Ran under systemd
The default is when RavenDB is used as a systemd daemon and controlled with systemd.
The following directory layout (initialized by the package installation process) and permissions is used in this scenario:

#### Filesystem locations and permissions

- settings - `/etc/ravendb/settings.json`

- security settings (e.g. certificate)  - `/etc/ravendb/security`

- data - `/var/lib/ravendb/data`

- logs  - `/var/log/ravendb/logs`

- audit logs  - `/var/log/ravendb/audit`

- binaries (current Server dir) - `/usr/lib/ravendb`

- rvn link - `/usr/bin/rvn -> /usr/lib/ravendb/rvn`

- ravendb - `/usr/bin/ravendb -> /usr/lib/ravendb/Raven.Server` 

- systemd unit file - `/lib/systemd/ravendb.service`

#### Permissions

| Location | Owner | Group | Permissions |
| - | - | - | - |
| `/usr/lib/ravendb/server` | root | ravendb | 550 |
| `/usr/bin/rvn -> /usr/lib/ravendb/rvn` | root | ravendb | 550 |
| `/usr/bin/ravendb -> /usr/lib/ravendb/Raven.Server` | root | ravendb | 550 |
| `/etc/ravendb/settings.json` | root | ravendb | 440 |
| `/etc/ravendb/security` | root | ravendb | 770 |
| `/var/lib/ravendb/data` | root | ravendb | 770 |
| `/var/log/ravendb/logs` | root | ravendb | 770 |
| `/var/log/ravendb/audit` | root | ravendb | 770 |

## Package lifecycle

### Installation

When installed and run the first time we start in `SetupMode` `Initial`. During package installation `postinst` script is going to print out the information about its future address. User should navigate there using a web browser and complete the Setup Wizard. Setup package obtained after installation on the first node can be used on node B. Server has `Setup.Certificate.Path` configuration option set. This is where it is going to save its initial server certificate.

`ravendb` user group can read and write the configuration in case the server needs to overwrite the settings file on Setup or renew the certificate.

### Removal

On a regular package removal (e.g. `apt-get rm`) only the binaries are going to be deleted. Configuration and the data are not touched.

### Purge

Data and configuration are removed when user decides to `purge` (`dpkg -P pkgname`) the package.

## Notes

To determine the Dependencies field value we need to:
```
$ apt show dotnet-runtime-5.0
Package: dotnet-runtime-5.0
Version: 5.0.2-1
Priority: standard
Section: libs
Maintainer: .NET Team <dotnetpackages@dotnetfoundation.org>
Installed-Size: 70.0 MB
Depends: dotnet-runtime-deps-5.0 (>= 5.0.2), dotnet-hostfxr-5.0 (>= 5.0.2)
Homepage: https://dot.net/core
Download-Size: 22.1 MB
APT-Sources: https://packages.microsoft.com/ubuntu/18.04/prod bionic/main amd64 Packages
```