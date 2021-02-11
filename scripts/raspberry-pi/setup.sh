#!/bin/bash

SCRIPT_TITLE="setup"
PKG_INSTALLER="apt-get" # supports : sudo ${PKG_INSTALLER} install <pkgname> 

CHK_PKGS=( "bzip2" "libunwind8" "tar" "libcurl3" )
DOTNET_DIR="dotnet"
RAVENDB_DIR="ravendb.5.2"
PROGS=( ${DOTNET_DIR} ${RAVENDB_DIR} )
RDB_DAEMON="ravendbd"

OP_SYSTEM_STARTUP=1
OP_SKIP_ERRORS=0
OP_REPORT=0
OP_INSTALL_LIBUV=0

NC='\033[0m'
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
PURPLE='\033[0;35m'
CYAN='\033[0;36m'
BLUE='\033[0;34m'

REPORT_FILE='/tmp/setup.ravendb.report.log'
OUT_FILE='/tmp/setup.ravendb.out.log'
REPORT_FLAG=0
REPORT_MAIL=""
REPORT_ATTACH=()
ATTACHMENTS=()

UPGRADE_SCRIPT="https://ravendb.net/raspberry-pi/updater.sh"

function printWelcome () {
	printf "\n\n${CYAN}RavenDB on Raspberry PI (Linux + dontnet core)\nSetup Script${BLUE} (v0.2) ${NC}\n"
	printf "${PURPLE}==============================================${NC}\n"
	TEST_ARCH=$(lsb_release -r 2>/dev/null | grep '16.04' | wc -l)$(uname -a | grep 'armv7l' | wc -l)
	if [ ${TEST_ARCH} != "11" ]; then
		printf "\n${RED}Invalid OS / Arch : ${NC}This script design to run on Ubuntu 16.04 for Raspberry Pi 3 (armv7l)\n\n"
		exit 1
	fi
	printf "(Enter 'sudo' passwd if required):\n"
	sudo echo "'sudo' passwd entered or not required"
}

function doUpgrade () {
	printf "\n\n${CYAN}RavenDB on Raspberry PI (Linux + dontnet core)\nUpgrade RavenDB) ${NC}\n"
	printf "${PURPLE}===========================================${NC}\n"
	TEST_ARCH=$(lsb_release -r 2>/dev/null | grep '16.04' | wc -l)$(uname -a | grep 'armv7l' | wc -l)
	if [ ${TEST_ARCH} != "11" ]; then
		printf "\n${RED}Invalid OS / Arch : ${NC}This script design to run on Ubuntu 16.04 for Raspberry Pi 3 (armv7l)\n\n"
		exit 1
	fi
	if [ $(ps -ef | grep corerun | grep Raven.Server.dll | wc -l) -gt 0 ]
	then
		printf "\n${RED}Please stop RavenDB before upgrading${NC}\n\n"
		exit 1
	fi
	printf "(Enter 'sudo' passwd if required):\n"
	sudo echo "'sudo' passwd entered or not required"

	printf "\n${CYAN}Downloading upgrade script...${NC}\n"
	wget -O upgrade.ravendb.sh ${UPGRADE_SCRIPT}
	status=$?
	if [ ${status} == 0 ]
	then
		sudo chmod +x upgrade.ravendb.sh
		printf "\n${CYAN}Executing upgrade script...${NC}\n"
		./upgrade.ravendb.sh
		status=$?
		if [ $status == 0 ]
		then
			printf "\n${GREEN}Upgrade script ended successfully${NC}\n\n"
		else
			printf "\n${RED}Error while executing upgrade script. rc=${status}${NC}\n\n"
		fi
		exit $status
	else
		rm -f upgrade.ravendb.sh >& /dev/null
		printf "\n${RED}ERROR : Failed to download upgrade files${NC}\n"
		exit $status
	fi	
}

function printHelp () {
	printWelcome
	printf "\nUsage : ${SCRIPT_TITLE}.sh [options]\n"
	printf "  Options:\n"
	printf "           --no-startup            : do not start ravendb on system startup\n"
	printf "	   --upgrade 		   : upgrade ravendb (network connection required)\n"
	printf "           --skip-errors           : do not exit on missing items, installation fails, build and test failures\n"
	printf "           --report=<email>        : send mail with the results of the build. --skip-errors will be automatically set\n"
	printf "           --help | -h             : this help info\n\n"

	exit 0
}

for i in "$@"
do
	case $i in
		--no-startup)
			OP_SYSTEM_STARTUP=0
			;;
		--skip-errors)
			OP_SKIP_ERRORS=1
			;;
		--report=*)
			OP_REPORT=1
			REPORT_MAIL="${i#*=}"
			OP_SKIP_ERRORS=1
			;;
		--upgrade)
			doUpgrade
			exit 0
			;;
		--help|-h)
			printHelp
			exit 0
			;;
	esac
done

RECURSIVE_CALL=0
function checkPackages () {
	if [ ${OP_REPORT} == 1 ]
	then
		printf "\n${PURPLE}Generating report for ${REPORT_MAIL}${NC}\n"
		rm -rf ${REPORT_FILE}
		echo "RavenDB Build Report for ${REPORT_MAIL}" >> ${REPORT_FILE}
		echo "===============================================================" >> ${REPORT_FILE}
		echo "`date +"%d/%m/%Y_%H:%M:%S"` ${SCRIPT_TITLE} started" >> ${REPORT_FILE}
	fi

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
		if [ ${OP_SKIP_ERRORS} == 0 ]
		then
			exit 101
		fi
		echoSkippingErros
	fi
}

function echoTestPkg () {
	printf "${NC}[     ] Checking existance of package ${PURPLE}$1${NC}..."
}

function echoExecProgram () {
	printf "${NC}[     ] executing ${PURPLE}$1${NC}..."
}

function echoTestProgram () {
	printf "${NC}[     ] Checking existance of command ${PURPLE}$1${NC}..."
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

function echoErrorPkgWithMsg () {
	printf " ${RED} $1${NC}"
	tput cub 9999
	tput cuf 1
	printf "${RED}ERROR${NC}\n"
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

function echoLabel () {
	tput cub 9999
	tput cuf 1
	printf "${PURPLE}$1${NC}"
}

function echoPercents () {
	if [ "$1" == "ERR" ];
	then
		tput cub 9999
		tput cuf 1
		printf "${RED} $1 ${NC}\n"
	elif [ "$1" == "OK" ];
	then
		tput cub 9999
		tput cuf 1
		printf "${GREEN} $1  ${NC}\n"
		return
	else	
		tput cub 9999
		tput cuf 1
		printf "${PURPLE}$1%%${NC}"
	fi
}

function echoSkippingErros () {
	printf "${YELLOW} --skip-errors set, ignoring failures${NC}\n"
	# sleep 1
}

function showDynamicProgress () {
	printf "${NC}[     ] $1.${PURPLE} $2${NC}"
	echoPercents " 0"
	cnt=0
	while [ `pidof $4 | wc | awk '{print $3}'` -ne 0 ];
	do
		numOfLines=`cat $3 | wc -l | awk '{print $1}'`
		let percents=(${numOfLines}*100)/$5
		if [ ${percents} -gt 99 ];
		then
			percents=99
		fi
		str=" ${percents}"
		if [ 10 -gt ${percents} ];
		then
			str=" ${percents}"
		fi
		echoPercents "${str}"
		let cnt=${cnt}+1
		if [ ${cnt} -gt 180 ];
		then
			return
		fi		
		sleep 1
	done
	echoPercents "100"
}

function buildRaven () {
	printf "\n${CYAN}Extracting files...${NC}\n"
	for CUR_FILE in "${PROGS[@]}"
	do
		echoExecProgram "Uncompressing ${CUR_FILE}"
		bunzip2 -k -f ${CUR_FILE}.tar.bz2
		status=$?
		if [ ${status} == 0 ];
		then
			echoSuccessExec
		else
			echoFailExec $status
			printf "\n${RED}Fatal error (bunzip2 -k) - stopping${NC}\n"
			exit 150
		fi
		echoExecProgram "Extracting ${CUR_FILE}"
		tar xvf ${CUR_FILE}.tar >& /dev/null
		status=$?
		if [ ${status} == 0 ]
		then
			echoSuccessExec
		else
			echoFailExec $status
			printf "\n${RED}Fatal error (tar cvf) - stopping${NC}\n"
			exit 151
		fi
		rm -f ${CUR_FILE}.tar >& /dev/null
	done
}

function sendMail () {
if [ ${OP_REPORT} == 1 ]
then
	printf "\n${PURPLE}Sending mail to ${REPORT_MAIL}${NC}\n"
	subjectstr="RavenDB Linux AutoBuild - "
	if [ ${REPORT_FLAG} == 0 ]
	then
		subjectstr="${subjectstr}PASSED!"
		echo "PASSED" > /tmp/mailToSend.txt
	else
		subjectstr="${subjectstr}FAILED!"
		if [ ${#ATTACHMENTS[@]} == 0 ]
		then
			echo "FAILED" > /tmp/mailToSend.txt
		else
			echo "FAILED (With Attachment Log)" > /tmp/mailToSend.txt
		fi
	fi
	cat ${REPORT_FILE} >> /tmp/mailToSend.txt
	echo " " >> /tmp/mailToSend.txt
	echo "`date +"%d/%m/%Y_%H:%M:%S"` =========== BUILD FINISHED =============== " >> /tmp/mailToSend.txt
	# msmtp -a gmail "${REPORT_MAIL}" -t < /tmp/mailToSend.txt
	if [ ${#ATTACHMENTS[@]} == 0 ]
        then
		mutt "${REPORT_MAIL}" -s "${subjectstr}" < /tmp/mailToSend.txt
	else
		mutt "${REPORT_MAIL}" -s "${subjectstr}" -a "${ATTACHMENTS[@]}" < /tmp/mailToSend.txt
	fi
	status=$?
	if [ ${status} == 0 ]
	then
		printf "\n${GREEN}Mail successfully sent${NC}\n\n"
	else
		printf "\n${RED}Mail was not sent. rc=${status}${NC}\n\n"
	fi

fi
}

function addToStartup () {
	if [ $OP_SYSTEM_STARTUP == 1 ]
	then
		echoExecProgram "Add RavenDB daemon to startup"
		sudo chmod +x ravendb.5.2/ravendb.watchdog.sh
		ESCAPED_PWD=$(pwd | sed 's/\//\\\//g' | sed 's/\&/\\\&/g')
		cat ${RAVENDB_DIR}/${RDB_DAEMON} | sed 's/RDB_DOTNET_PATH/'${ESCAPED_PWD}'\/'${DOTNET_DIR}'/g' | sed 's/RDB_RAVENDB_PATH/'${ESCAPED_PWD}'\/'${RAVENDB_DIR}'/g' | sed 's/RDB_USERNAME/'${USER}'/g' > ${RDB_DAEMON}.config
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
	fi
}

printWelcome

checkPackages

buildRaven

addToStartup

sendMail

printf "${NC}\n${GREEN}Done. Enjoy RavenDB :)${NC}\n"

exit 0



