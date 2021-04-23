#!/bin/bash

function is_systemd {
    command -v systemctl &> /dev/null;
}

function test_raven_local {

adduser --disabled-login --gecos 'Package test' tester
usermod -a -G ravendb tester

mkdir -p /test/ravendb-config
chown -R tester:tester /test

su --preserve-environment - tester <<TEST
cd /test

echo "{}" > /etc/ravendb/settings.json
# --preserve-environment does not preserve HOME, which we need to override
export HOME=/var/lib/ravendb

/usr/lib/ravendb/server/Raven.Server -c /etc/ravendb/settings.json & 
rvnpid=\$!

trap "rc=\$?; pgrep \$rvnpid && kill -9 \$rvnpid 2>&1 >/dev/null; echo \$rc; exit \$rc" EXIT

sleep 7 

curl -Ss 'http://localhost:8080/admin/databases?name=Test&replicationFactor=1' \
  --retry 3 \
  -X 'PUT' \
  --data-raw '{"DatabaseName":"Test","Settings":{},"Disabled":false,"Encrypted":false,"Topology":{"DynamicNodesDistribution":false}}' \
  --compressed \
  --insecure >/dev/null

sleep 3

test -d \${RAVEN_DataDir}/System && echo "System dir: OK" || exit 1
test -d \${RAVEN_DataDir}/Databases && echo "Databases dir: OK" || exit 1
test -d \${RAVEN_DataDir}/Databases/Test && echo "Test database dir: OK" || exit 1
pgrep Raven.Server >/dev/null && echo "RavenDB server process: OK" || exit 1

kill -SIGTERM \$rvnpid
wait \$rvnpid
rvnRc=\$?
echo "Server exited with code \$rvnRc."
exit \$rvnRc
TEST

if [ $? -ne 0 ]; then
    echo "Running RavenDB as regular user failed."
    exit 1
fi

echo "All OK."

}


function test_raven_systemd {

echo "Change RavenDB settings.json:"
su - <<SETUP

cat <<CONFIG > /etc/ravendb/settings.json
{
    "ServerUrl": "http://0.0.0.0:8080",
    "Setup.Mode": "None",
    "Security.UnsecuredAccessAllowed": "PublicNetwork",
    "Logs.RetentionTimeInHrs": 336,
    "Security.AuditLog.Compress": true,
    "License.Eula.Accepted": true
}
CONFIG

chown root:ravendb /etc/ravendb/settings.json
chmod 660 /etc/ravendb/settings.json

cat /etc/ravendb/settings.json

systemctl restart ravendb

SETUP

su - <<TEST

systemctl status ravendb
set -e

sleep 5 

curl -Ss 'http://localhost:8080/admin/databases?name=Test&replicationFactor=1' \
  --retry 3 \
  -X 'PUT' \
  --data-raw '{"DatabaseName":"Test","Settings":{},"Disabled":false,"Encrypted":false,"Topology":{"DynamicNodesDistribution":false}}' \
  --compressed \
  --insecure >/dev/null

sleep 3

test -d /var/lib/ravendb/data/System
echo "System dir: OK"

test -d /var/lib/ravendb/data/Databases
echo "Databases dir: OK"

test -d /var/lib/ravendb/data/Databases/Test
echo "Test database dir: OK"

pgrep Raven.Server >/dev/null
echo "RavenDB server process: OK"

TEST

su - <<ROOT

if ! systemctl is-active ravendb; then
    echo "RavenDB service not active"
    exit 1
fi

systemctl status ravendb
journalctl --no-pager --since '5 minutes ago' -q -u ravendb
ROOT

if [ $? -ne 0 ]; then
    echo "Running RavenDB as regular user failed."
    exit 1
fi

echo "All OK."

}

function assert_ravendb_installed {

    local binPaths=(
        /usr/bin/rvn
        /usr/lib/ravendb/server/Raven.Server
    )
    
    local filePaths=(
        /usr/bin/rvn 
        /etc/ravendb/settings.json
        /etc/ravendb/security/master.key
        /usr/lib/ravendb/server/Raven.Server
        /etc/ld.so.conf.d/ravendb.conf
    )
    
    local dirPaths=(
        /usr/lib/ravendb/server
        /var/lib/ravendb/data
        /etc/ravendb/security
        /var/log/ravendb/logs
        /var/log/ravendb/audit
    )

    for p in ${filePaths[@]}; do
        test -f "$p" \
            || (echo "$p is not a file or does not exist." && return 1)
    done || return

    echo "Files: OK"

    for p in ${binPaths[@]}; do
        test -x "$p" \
            || (echo "$p is not executable." && return 1)
    done || return

    echo "Executable bits set: OK"

    for p in ${dirPaths[@]}; do
        test -d "$p" \
            || (echo "$p is not a directory." && return 1)
    done || return 

    echo "Dirs: OK"

    serviceFile=/usr/lib/ravendb/ravendb.service
    if is_systemd && ! systemctl cat ravendb.service >& /dev/null; then 
        echo "$serviceFile does not exist."
        return 1
    else
        echo "Systemd service: OK"
    fi

    id -u ravendb || (echo "ravendb user does not exist" && return 1)
    echo "ravendb user: OK"

    getent group ravendb || (echo "ravendb group does not exist." && return 1)
    echo "ravendb group: OK"

    echo "DEB package installation succeeded."
}

function assert_ravendb_uninstalled {

    local filesKept=(
        /etc/ravendb/settings.json
        /etc/ravendb/security/master.key
        /etc/ld.so.conf.d/ravendb.conf
    )

    local dirsKept=(
        /etc/ravendb/security
        /var/log/ravendb/logs
        /var/log/ravendb/audit
        /var/lib/ravendb/data
    )

    local filesDeleted=(
        /usr/bin/rvn
        /usr/lib/ravendb/server/Raven.Server
    )
    
    local dirsDeleted=(
        /usr/lib/ravendb/server
    )

    if is_systemd; then
        systemd_data_dirs=( /var/lib/ravendb/data/Databases /var/lib/ravendb/data/System )
        dirsKept=("${dirsKept[@]}" "${systemd_data_dirs[@]}")

        systemd_unit_files=( /lib/systemd/system/ravendb.service )
        filesDeleted=("${filesDeleted[@]}" "${systemd_unit_files[@]}")
    fi

    for p in ${filesDeleted[@]}; do
        test ! -f "$p" \
            || (echo "$p still exists." && return 1)
    done || return 

    echo "Files deleted: OK"

    for p in ${dirsDeleted[@]}; do
        (test ! -d "$p" && test ! -f "$p") \
            || (echo "$p still exists." && return 1)
    done || return

    echo "Dirs deleted: OK"

    for p in ${filesKept[@]}; do
        test -f "$p" \
            || (echo "$p does not exist." && return 1)
    done || return

    echo "Files kept: OK"

    for p in ${dirsKept[@]}; do
        test -d "$p" \
            || (echo "$p directory does not exist." && return 1)
    done || return

    echo "Dirs kept: OK"

    echo "DEB package removal succeeded."
}

function test_package_local {
    pkgFilename=$1

    echo "Testing the package $pkgFilename"
    
    echo "Install package."
    dpkg -i $pkgFilename
    
    set -e
    export DEBIAN_FRONTEND=noninteractive 
    apt-get -y -f install

    if ! assert_ravendb_installed; then
        return 1
    fi

    if ! test_raven_local; then
        return 1
    fi

    echo "Remove package."
    dpkg -r ravendb

    if ! assert_ravendb_uninstalled; then
        echo "ravendb package removal failed. See errors above."
        return 1
    fi

    echo "Package works fine on Docker."
    set +e
}

function test_package_systemd {
    pkgFilename=$1

    echo "Testing the package $pkgFilename"
    echo "Install package."
    dpkg -i $pkgFilename

    set -e
    export DEBIAN_FRONTEND=noninteractive
    if ! apt-get -y -f install; then
        return 1
    fi


    if ! assert_ravendb_installed; then
        return 1
    fi

    if ! test_raven_systemd; then
        return 1
    fi

    echo "Remove package."
    dpkg -r ravendb

    if ! assert_ravendb_uninstalled; then
        return 1
    fi

    if ! test_package_purged ravendb; then
        return 1
    fi

    echo "Package works fine as a systemd daemon."
    set +e
}

function test_package_purged {
    dpkg --purge $1
    
    local purged=(
        /etc/ravendb
        /etc/ravendb/security
        /etc/ravendb/settings.json
        /var/log/ravendb/logs
        /var/log/ravendb/audit
        /var/lib/ravendb
        /var/lib/ravendb/data
        /usr/lib/ravendb/server/Raven.Server
        /etc/ld.so.conf.d/ravendb.conf
    )

    for p in ${purged[@]}; do
        test ! -e "$p" \
            || (echo "$p still exists." && return 1)
    done || return

    echo "All files removed on purge: OK"

    echo "DEB package purge succeeded."
}
