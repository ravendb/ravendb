#!/bin/bash

SCRIPT_TITLE="install-daemon"
PKG_INSTALLER="apt-get" # supports : sudo ${PKG_INSTALLER} install <pkgname> 

CHK_PKGS=( "bzip2" "tar" )
RDB_DAEMON="ravendbd"

NC='\033[0m'
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
PURPLE='\033[0;35m'
CYAN='\033[0;36m'
BLUE='\033[0;34m'

REPORT_FILE='/tmp/install-daemon.ravendb.report.log'
OUT_FILE='/tmp/install-daemon.ravendb.out.log'

RAVEN_UBUNTU_PKG="../../artifacts/ubuntu.`lsb_release -r | awk '{print $2}'`-x64"
RDB_WATCHDOG="ravendb.watchdog.sh"

function printWelcome () {
	printf "\n\n${CYAN}RavenDB - Install Daemon (Linux)\n${SCRIPT_TITLE} Script${BLUE} (v0.1) ${NC}\n"
	printf "${PURPLE}========================${NC}\n"
	TEST_ARCH=$(lsb_release -r 2>/dev/null | egrep "14.04|16.04" | wc -l)$(uname -a | grep -i 'ubuntu' | wc -l)
	if [ ${TEST_ARCH} != "11" ]; then
		printf "\n${RED}Invalid OS / Arch : ${NC}This script design to run on Ubuntu 14.04 or Ubuntu 16.04 Only\n\n"
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
	fi
}

function echoTestPkg () {
	printf "${NC}[     ] Checking existance of package ${PURPLE}$1${NC}..."
}

function echoExecProgram () {
        printf "${NC}[     ] executing ${PURPLE}$1${NC}..."
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

function echoSuccessExec () {
	printf " ${NC} Succeeded!${NC}"
	tput cub 9999
	tput cuf 2
	printf "${GREEN}OK${NC}\n"
}

function echoFailExec () {
	printf " ${RED} Failed! rc=$1${NC}"
	tput cub 9999
	tput cuf 1
	printf "${RED}ERROR${NC}\n"
}



function addToStartup () {
		echoExecProgram "Add RavenDB daemon to startup"
		ESCAPED_PWD=$(pwd | sed 's/\//\\\//g' | sed 's/\&/\\\&/g')
		DOTNET_DIR=$(echo "`which dotnet`" | sed 's/\//\\\//g' | sed 's/\&/\\\&/g')
		RAVENDB_DIR=$(echo -n "${ESCAPED_PWD}" && echo "/${RAVEN_UBUNTU_PKG}/package/Server" | sed 's/\//\\\//g' | sed 's/\&/\\\&/g')
		cat ${RDB_DAEMON} | sed 's/RDB_DOTNET_PATH/'${DOTNET_DIR}'/g' | sed 's/RDB_RAVENDB_PATH/'${RAVENDB_DIR}'/g' | sed 's/RDB_USERNAME/'${USER}'/g' > ${RDB_DAEMON}.config

		cat ${RDB_WATCHDOG} | sed 's/RDB_DOTNET_PATH/'${DOTNET_DIR}'/g' | sed 's/RDB_RAVENDB_PATH/'${RAVENDB_DIR}'/g' > ${RDB_WATCHDOG}.config
		mv ${RDB_WATCHDOG}.config `pwd`/${RAVEN_UBUNTU_PKG}/package/Server/${RDB_WATCHDOG}
		sudo chmod +x `pwd`/${RAVEN_UBUNTU_PKG}/package/Server/${RDB_WATCHDOG}
		sudo mv ${RDB_DAEMON}.config /etc/init.d/${RDB_DAEMON} >& /dev/null

		sudo chmod +x /etc/init.d/${RDB_DAEMON} >& /dev/null
		sudo update-rc.d ${RDB_DAEMON} defaults
		status=$?
		if [ ${status} == 0 ];
		then
			echoSuccessExec
		else
			echoFailExec $status
			exit 152
		fi
		echoExecProgram "Start RavenDB daemon (wait upto 40s)"
		sudo service ${RDB_DAEMON} restart >& /tmp/${RDB_DAEMON}.setup.log &
		TIMEWAIT=40
		SAWTHELIGHT=0
		while [ ${TIMEWAIT} -gt 0 ];
		do
			sleep 1
			SAWTHELIGHT=$(sudo service ${RDB_DAEMON} status | tail -1 | grep "Running as Service" | wc -l)
			if [ ${SAWTHELIGHT} == 1 ]
			then
				break;
			fi
			TIMEWAIT=$(expr ${TIMEWAIT} - 1)
			echo -n "."
		done	
		if [ ${SAWTHELIGHT} == 1 ];
		then
			echoSuccessExec
		else
			echoFailExec $status
			exit 153
		fi
}

printWelcome

checkPackages

addToStartup

printf "${NC}\n${GREEN}Done. RavenDB installed as autostart daemon${NC}\n"

exit 0



