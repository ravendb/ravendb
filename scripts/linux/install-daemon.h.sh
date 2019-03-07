function get_os { # returns OS, VER, IS_LINUX, IS_ARM, IS_MAC, IS_32BIT
    IS_LINUX=$(uname -s | grep Linux  | wc -l)
	IS_ARM=$(uname -m | grep arm    | wc -l)
	IS_MAC=$(uname -s | grep Darwin | wc -l)
	IS_32BIT=$(file /bin/bash | grep 32-bit | wc -l)

    if [ -f /etc/os-release ]; then
        # freedesktop.org and systemd
        . /etc/os-release
        OS=$NAME
        VER=$VERSION_ID
    elif type lsb_release >/dev/null 2>&1; then
        # linuxbase.org
        OS=$(lsb_release -si)
        VER=$(lsb_release -sr)
    elif [ -f /etc/lsb-release ]; then
        # For some versions of Debian/Ubuntu without lsb_release command
        . /etc/lsb-release
        OS=$DISTRIB_ID
        VER=$DISTRIB_RELEASE
    elif [ -f /etc/debian_version ]; then
        # Older Debian/Ubuntu/etc.
        OS=Debian
        VER=$(cat /etc/debian_version)
    elif [ -f /etc/SuSe-release ]; then
        # Older SuSE/etc.
        # ...
        OS=$(uname -s)
        VER=$(uname -r)
    elif [ -f /etc/redhat-release ]; then
        # Older Red Hat, CentOS, etc.        
        # ...
        OS=$(uname -s)
        VER=$(uname -r)
    else
        # Fall back to uname, e.g. "Linux <version>", also works for BSD, etc.
        OS=$(uname -s)
        VER=$(uname -r)
    fi
}

function get_installation_type { # returns IS_UPSTART, IS_SYSTEMD
    if [ "${OS}" == "Ubuntu" ] && ([ "${VER}" == "14.04" ] || [ "${VER}" == "16.04" ]); then
        IS_UPSTART=1
    elif [ "${OS}" == "Ubuntu" ] && [ "${VER}" == "18.04" ]; then
        IS_SYSTEMD=1
    else
        echo -ne "${C_L_MAGENTA}Select type of installation: Upstart or Systemd [u/s]: ${NC}"
        read -n 1 ANS
        if [ "${ANS}" == "u"] || [ "${ANS}" == "U" ]; then
            IS_UPSTART=1
        elif [ "${ANS}" == "s"] || [ "${ANS}" == "S" ]; then
            IS_SYSTEMD=1
        else
            echo -e "${ERR_STRING}Illegal selection${NC}"
            exit 1
        fi
    fi
}

function install_ravendb_systemd {
    echo ""
    echo -e "${C_YELLOW}Install ravendb.service"
    echo -e "=======================${NC}"
    echo ""
    cp ravendb.service ravendb.service.tmp
    sed -i 's/RAVENDB_USERNAME/'${USER}'/' ravendb.service.tmp
    RUNDIR="$1"
    echo -ne "${C_L_MAGENTA}Enter path of run.sh executable (or leave blank for ${C_L_CYAN}${RUNDIR}${C_L_MAGENTA}):"
    read ANS
    if [ "${ANS}" != "" ]; then RUNDIR="${ANS}"; fi
    ESC_RUNDIR="$(echo ${RUNDIR} | sed 's/\//\\\//g')"
    echo -e "${C_L_CYAN}Selected dir=${ESC_RUNDIR}"
    sed -i 's/RAVENDB_PATH/'${ESC_RUNDIR}'/' ravendb.service.tmp
    echo ""
    echo -e "${C_L_YELLOW}If prompt, enter su credentials...${NC}"
    sudo mv ravendb.service.tmp /etc/systemd/system/ravendb.service
    sudo systemctl daemon-reload
    sudo systemctl enable ravendb.service
    echo " "
    echo "Starting service..."
    sudo systemctl restart ravendb.service
    echo " "
    sleep 5
    sudo systemctl status ravendb.service
    echo " "
    echo "For details try journalctl -u ravendb.service"
    echo ""

}