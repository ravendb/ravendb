#!/bin/bash
echo ""

DEFPATH=${PWD}

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

. install-daemon.h.sh

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
    . install-daemon-upstart.sh
else
    install_ravendb_systemd ${DEFPATH}
fi

echo ""
echo -e "${C_L_BLUE}Done."
echo ""

