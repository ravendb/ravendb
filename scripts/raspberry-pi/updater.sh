#!/bin/bash

SCRIPT_TITLE="updater"
PKG_INSTALLER="apt-get" # supports : sudo ${PKG_INSTALLER} install <pkgname> 

CHK_PKGS=( "libunwind8" "libcurl3" )

NC='\033[0m'
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
PURPLE='\033[0;35m'
CYAN='\033[0;36m'
BLUE='\033[0;34m'

REPORT_FILE='/tmp/updater.ravendb.report.log'
OUT_FILE='/tmp/updater.ravendb.out.log'

function printWelcome () {
	printf "\n\n${CYAN}RavenDB on Raspberry PI (Linux + dontnet core)\nUpdater Script${BLUE} (v0.1) ${NC}\n"
	printf "${PURPLE}==============================================${NC}\n"
	TEST_ARCH=$(lsb_release -r 2>/dev/null | grep '16.04' | wc -l)$(uname -a | grep 'armv7l' | wc -l)
	if [ ${TEST_ARCH} != "11" ]; then
		printf "\n${RED}Invalid OS / Arch : ${NC}This script design to run on Ubuntu 16.04 for Raspberry Pi 3 (armv7l)\n\n"
		exit 1
	fi
	printf "(Enter 'sudo' passwd if required):\n"
	sudo echo "'sudo' passwd entered or not required"
}

RECURSIVE_CALL=0
function checkPackages () {
	printf "\n${PURPLE}Generating report for ${REPORT_MAIL}${NC}\n"
	rm -rf ${REPORT_FILE}
	echo "RavenDB Build Report for ${REPORT_MAIL}" >> ${REPORT_FILE}
	echo "===============================================================" >> ${REPORT_FILE}
	echo "`date +"%d/%m/%Y_%H:%M:%S"` ${SCRIPT_TITLE} started" >> ${REPORT_FILE}

	printf "\n${CYAN}Checking Packages:${NC}\n"
	pkgsNotInstalled=()
	foundMissingPkgs=0
	for i in "${CHK_PKGS[@]}"
	do
		echoTestPkg $i
		pkg=$(dpkg --get-selections | grep -v deinstall | cut -f1 | cut -f1 -d':' | egrep "^$i$" )
		if [ -z "$pkg" ] 
		then
			pkgsNotInstalled+=($i)
			echoErrorPkg ${RECURSIVE_CALL}
			foundMissingPkgs=1
		else
			echoOkPkg
		fi
	done
	if [ ${foundMissingPkgs} == 0 ]
	then
		printf "\n${GREEN} All needed packages are installed${NC}\n\n"		
	else
		echo "`date +"%d/%m/%Y_%H:%M:%S"` FATAL ERROR - Missing packages : ${pkgsNotInstalled[@]}" >> ${REPORT_FILE}
		ERR_COLOR="${RED}"
		if [ ${RECURSIVE_CALL} == 0 ]
		then
			ERR_COLOR="${YELLOW}"
		fi
		printf "\n${ERR_COLOR} Missing packages : "		
		printf "${pkgsNotInstalled[@]}${NC}\n"

		if [ ${RECURSIVE_CALL} == 0 ]
		then			
			for i in "${pkgsNotInstalled[@]}"
			do
				printf "${CYAN}Installing ${i}:${NC}\n"
				printf "(about to 'sudo' ${PKG_INSTALLER}... enter password to allow installation)\n"
				sudo ${PKG_INSTALLER} -y install ${i}
			done
			RECURSIVE_CALL=1
			printf "\n${YELLOW}About to retry test packages existance${NC}\n\n"
			checkPackages
			return
		fi
		exit 101
	fi
}

function echoTestPkg () {
	printf "${NC}[     ] Checking existance of package ${PURPLE}$1${NC}..."
}

function echoErrorPkg () {
	ERR_COLOR="${RED}"
	ERR_MESSG="ERROR"
	if [ "a$1" == "a0" ] 
	then 
		ERR_COLOR="${YELLOW}"
		ERR_MESSG="WARN "
	fi
	printf " ${ERR_COLOR} Not installed!${NC}"
	tput cub 9999
	tput cuf 1
	printf "${ERR_COLOR}${ERR_MESSG}${NC}\n"
}

function echoOkPkg () {
	printf " ${NC} Installed!${NC}"
	tput cub 9999
	tput cuf 2
	printf "${GREEN}OK${NC}\n"
}


printWelcome

checkPackages

exit 0



