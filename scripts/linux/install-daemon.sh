#!/bin/bash
echo ""

DEFPATH=$( cd `dirname $0` && pwd )

NC="\\e[39m"
C_BLACK="\\e[30m"
C_RED="\\e[31m"
C_GREEN="\\e[32m"
C_YELLOW="\\e[33m"
C_BLUE="\\e[34m"
C_MAGENTA="\\e[35m"
C_CYAN="\\e[36m"
C_L_GRAY="\\e[37m"
C_D_GRAY="\\e[90m"
C_L_RED="\\e[91m"
C_L_GREEN="\\e[92m"
C_L_YELLOW="\\e[93m"
C_L_BLUE="\\e[94m"
C_L_MAGENTA="\\e[95m"
C_L_CYAN="\\e[96m"
C_WHITE="\\e[97m"

NT="\\e[0m"
T_BOLD="\\e[1m"
T_UL="\\e[4m"
T_BLINK="\\e[5m"

ERR_STRING="${C_L_RED}${T_BOLD}Error: ${NT}${C_L_RED}"

IS_UPSTART=0
IS_SYSTEMD=0

EXEC_USER=${USER:-$(whoami)}

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
    if [ "${OS}" == "Ubuntu" ] && ([ "${VER}" == "14.04" ]); then
        IS_UPSTART=1
    elif [ "${OS}" == "Ubuntu" ] && ([ "${VER}" == "18.04" ] || [ "${VER}" == "16.04" ]); then
        IS_SYSTEMD=1
    elif [ $(which systemctl | wc -l) -eq 1 ]; then
        IS_SYSTEMD=1
    else
        if [ -t 0 ]; then
            read -n 1 -p "${C_L_MAGENTA}Select type of installation: Upstart or Systemd [u/s]: ${NC}" ANS
            if [ "$?" -ne 0 ]; then
                echo -e "${ERR_STRING}Could not determine type of installation.${NC}"
                exit 1
            fi
        else
            ANS=''
        fi

        if [ "${ANS}" == "u"] || [ "${ANS}" == "U" ]; then
            IS_UPSTART=1
        elif [ "${ANS}" == "s"] || [ "${ANS}" == "S" ]; then
            IS_SYSTEMD=1
        else
            echo -e "${ERR_STRING}Could not determine type of installation.${NC}"
            exit 1
        fi
    fi
}

function install_ravendb_systemd {
    echo ""
    echo -e "${C_YELLOW}Install ravendb.service"
    echo -e "=======================${NC}"
    echo ""
    RUNDIR="$1"

    if [ -t 0 ]; then
        echo -ne "${C_L_MAGENTA}Enter path of run.sh executable (or leave blank for ${C_L_CYAN}${RUNDIR}${C_L_MAGENTA}):" 
        read RUNSH_PATH
        if [ "$?" -eq 0 ]; then
            RUNDIR="${RUNSH_PATH:-$RUNDIR}"
        fi
    else
        echo "run.sh path: $RUNDIR"
    fi

    ESC_RUNDIR="$(echo ${RUNDIR} | sed 's/\//\\\//g')"
    echo -e "${C_L_CYAN}Selected dir=${RUNDIR}"
    echo ""
    echo -e "${C_L_YELLOW}If prompt, enter su credentials...${NC}"

    read -r -d '' RAVENDB_SYSTEMD_SERVICE_CONF << END
[Unit]
Description=RavenDB v5.2
After=network.target

[Service]
LimitCORE=infinity
LimitNOFILE=65535
LimitRSS=infinity
LimitAS=infinity
User=USER
StartLimitBurst=0
Restart=on-failure
Type=simple
TimeoutStopSec=300
ExecStart=RAVENDB_DIR/run.sh

[Install]
WantedBy=multi-user.target
END

    export RAVENDB_SYSTEMD_SERVICE_CONF
    export SYSTEMD_SERVICE_FILE=/etc/systemd/system/ravendb.service
    sudo -E bash -c 'echo "$RAVENDB_SYSTEMD_SERVICE_CONF" > "$SYSTEMD_SERVICE_FILE"'
    sudo sed -i "s/USER/$EXEC_USER/g" "$SYSTEMD_SERVICE_FILE"
    sudo sed -i "s/RAVENDB_DIR/$ESC_RUNDIR/g" "$SYSTEMD_SERVICE_FILE"

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

echo -e "${C_L_GREEN}${T_BOLD}Install RavenDB Daemon/Service${NC}${NT}"
echo -e "${C_D_GRAY}==============================${NC}"
echo ""

get_os

echo -e "${C_L_BLUE}Username:\t${C_L_CYAN}${USER}${NC}"
echo -e "${C_L_BLUE}OS      :\t${C_L_CYAN}${OS}${NC}"
echo -e "${C_L_BLUE}Version :\t${C_L_CYAN}${VER}${NC}"
echo -e "${C_L_BLUE}Info    :\t${C_L_CYAN}Linux(${IS_LINUX}),Arm(${IS_ARM}),Mac(${IS_MAC}),32Bit(${IS_32BIT})${NC}"

get_installation_type

echo -ne "${C_L_BLUE}Installation type   :\t${C_L_CYAN}"
[ ${IS_SYSTEMD} -eq 1 ] && echo -e "Systemd${NC}"
[ ${IS_UPSTART} -eq 1 ] && echo -e "Upstart${NC}"
if [ $(expr ${IS_UPSTART} + ${IS_SYSTEMD}) -ne 1 ]; then
    echo -n "${ERR_STRING}: Internal error (${IS_UPSTART}, ${IS_SYSTEMD})${NC}"
    exit 1
fi
echo -ne "${C_L_BLUE}Default install path:\t${C_L_CYAN}${DEFPATH}${NC}"

echo ""

if [ ${IS_UPSTART} -eq 1 ]; then
    echo "Upstart init system is not supported by this installer."
    echo "Please proceed with manual installation."
else
    install_ravendb_systemd "${DEFPATH}"
fi

echo ""
echo -e "${C_L_BLUE}Done."
echo ""

