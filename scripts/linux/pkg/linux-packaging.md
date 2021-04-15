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

#### Setup 

RavenDB is set up using RavenDB Setup Wizard running on http://localhost:53700. 
You can read more on setting up the server here: https://ravendb.net/docs/article-page/5.1/csharp/start/installation/setup-wizard

##### On a headless server / SSH-only

If you only have SSH access please open up port `53700` (RavenDB setup), `443` (RavenDB HTTPS server) and `38888` (RavenDB TCP server) on the target machine. 
Then set up port tunneling through SSH - tunnel port `53700` from `localhost` of the target machine to `localhost:8080` of the SSH client machine:
```
ssh -N -L localhost:8080:localhost:53700 ubuntu@target.machine.com
```

If you connect using the key, you'll need to supply it with `-i` option.

This will allow you to open the RavenDB Setup Wizard on http://localhost:8080 and proceed with setting up RavenDB server on a target machine.

### Removal

On a regular package removal (e.g. `apt-get rm`) only the binaries are going to be deleted. Configuration and the data are not touched.

### Purge

Data and configuration are removed when user decides to `purge` (`dpkg -P pkgname`) the package.
