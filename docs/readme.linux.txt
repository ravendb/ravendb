Startup instructions for RavenDB on Linux
=========================================

* RavenDB as a Console Application
Open bash terminal
Type:
    chmod +x run.sh
    ./run.sh

* RavenDB as Daemon (systemd - applies to Ubuntu 16.04)
Open bash terminal, and create file /etc/systemd/system/ravendb.service, using super user permissions, containing:
    [Unit]
    Description=RavenDB v6.2
    After=network.target

    [Service]
    LimitCORE=infinity
    LimitNOFILE=65535
    LimitRSS=infinity
    LimitAS=infinity
    LimitMEMLOCK=infinity
    TasksMax=infinity
    StartLimitBurst=0
    AmbientCapabilities=CAP_NET_BIND_SERVICE  
    Restart=on-failure
    Type=exec
    TimeoutStopSec=300
    User=<desired user>
    ExecStart=/path/to/RavenDB/run.sh

    [Install]
    WantedBy=multi-user.target

Note: Replace in the above text the username "User=<desired user>" and set path in "ExecStart"

Then register the service and enable it on startup by typing:
    systemctl daemon-reload
    systemctl enable ravendb.service

Start the service:
    systemctl start ravendb.service

View its status using:
    systemctl status ravendb.service
    or
    journalctl -f -u ravendb.service

* Setup
Open browser, if not opened automatically, at url printed in "Server available on: <url>"
Follow the web setup instructions at: https://docs.ravendb.net/6.2/start/installation/setup-wizard/overview

* Upgrading to a New Version
Follow the upgrade instructions available at: https://docs.ravendb.net/6.2/start/installation/upgrading-to-new-version



