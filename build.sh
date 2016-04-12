#!/bin/bash

PKG_INSTALLER="apt-get" # supports : sudo ${PKG_INSTALLER} install <pkgname>   (apt-get and yum currently supported)

CLR_VER="1.0.0-rc1-update2"
CLR_RUNTIME="coreclr"
CLR_ARCH="x64"

TEST_DIRS=( `find test -name project.json -printf "%h\n"` )
BUILD_DIRS=(` find src -name project.json -printf "%h\n"` )
CHK_PKGS=( "unzip" "curl" "libunwind8" "gettext" "libssl-dev" "libcurl4-openssl-dev" "zlib1g" "libicu-dev" "uuid-dev" )


OP_INSTALL_PKGS=0
OP_INSTALL_DNX=0
OP_SKIP_ERRORS=0
OP_REPORT=0
OP_SKIP_TESTS=0

NC='\033[0m'
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
PURPLE='\033[0;35m'
CYAN='\033[0;36m'
BLUE='\033[0;34m'

REPORT_FILE='/tmp/build.report'
OUT_FILE='/tmp/build.out'
REPORT_FLAG=0
REPORT_MAIL=""
REPORT_ATTACH=()
ATTACHMENTS=()

function printWelcome () {
	printf "\n\n${CYAN}RavenDB (Linux) Build Script${BLUE} (v0.4) ${NC}\n"
	printf "${PURPLE}============================${NC}\n"
}

function printHelp () {
	printWelcome
	printf "\nUsage : build.sh [options]\n"
	printf "  Options:\n"
	printf "           --install-pkgs          : if found missing packages - try to install using packager installer\n"
	printf "           --install-dnx           : try to install dnvm and compile libuv if missing\n"
	printf "           --skip-errors           : do not exit on missing items, installation fails, build and test failures\n"
	printf "           --skip-tests            : do not perform tests\n"
	printf "           --clr-version=<version> : set clr version to install and use (default : ${CLR_VER})\n"
	printf "           --clr-runtime=<runtime> : set clr runtime to install and use (default : ${CLR_RUNTIME})\n"
	printf "           --report=<email>        : send mail with the results of the build. --skip-errors will be automatically set\n"
	printf "           --help | -h             : this help info\n\n"

	exit 0
}

for i in "$@"
do
	case $i in
		--install-pkgs)
			OP_INSTALL_PKGS=1
			;;
		--install-dnx)
			OP_INSTALL_DNX=1
			;;
		--skip-errors)
			OP_SKIP_ERRORS=1
			;;
		--skip-tests)
			OP_SKIP_TESTS=1
			;;
		--pkg-installer=*)
			PKG_INSTALLER="${i#*=}"
			shift
			;;
		--clr-version=*)
			CLR_VER="${i#*=}"
			shift
			;;
		--clr-runtime=*)
			CLR_RUNTIME="${i#*=}"
			shift
			;;
		--clr-arch=*)
			CLR_ARCH="${i#*=}"
			shift
			;;
		--report=*)
			OP_REPORT=1
			REPORT_MAIL="${i#*=}"
			OP_SKIP_ERRORS=1
			;;
		--help|-h)
			printHelp
			exit 0
			;;
	esac
done

if [[ ${OP_REPORT} == 1 && ${OP_INSTALL_PKGS} == 1 ]]
then
	echo "\n${RED}Cannot build with --report and --install-pkgs. Exiting..${NC}\n\n"
	exit 1
fi

if [[ ${OP_REPORT} == 1 && ${OP_INSTALL_DNX} == 1 ]]
then
        echo "\n${RED}Cannot build with --report and --install-dnx Exiting..${NC}\n\n"
        exit 1
fi



RECURSIVE_CALL=0
function checkPackages () {
	if [ ${OP_REPORT} == 1 ]
	then
		printf "\n${PURPLE}Generating report for ${REPORT_MAIL}${NC}\n"
		rm -rf ${REPORT_FILE}
		echo "RavenDB Build Report for ${REPORT_MAIL}" >> ${REPORT_FILE}
		echo "===============================================================" >> ${REPORT_FILE}
		echo "`date +"%d/%m/%Y_%H:%M:%S"` Build started" >> ${REPORT_FILE}
	fi

	printf "\n${BLUE}Checking Packages:${NC}\n"
	pkgsNotInstalled=()
	foundMissingPkgs=0
	for i in "${CHK_PKGS[@]}"
	do
		echoTestPkg $i
		pkg=$(dpkg --get-selections | grep -v deinstall | cut -f1 | cut -f1 -d':' | egrep "^$i$" )
		if [ -z "$pkg" ] 
		then
			pkgsNotInstalled+=($i)
			echoErrorPkg
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
		printf "\n${RED} Missing packages : "		
		printf "${pkgsNotInstalled[@]}${NC}\n"

		if [ ${RECURSIVE_CALL} == 0 ] && [ ${OP_INSTALL_PKGS} == 1 ]
		then			
			for i in "${pkgsNotInstalled[@]}"
			do
				printf "${CYAN}Installing ${i}:${NC}\n"
				printf "(about to 'sudo' ${PKG_INSTALLER}... enter password to allow installation)\n"
				sudo ${PKG_INSTALLER} -y install ${i}
			done
			RECURSIVE_CALL=1
			printf "\n{YELLOW}About to retry test packages existance${NC}\n\n"
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

function checkDnx () {
	printf "\n${BLUE}Checking DNX:${NC}\n"	
	[ -s "${HOME}/.dnx/dnvm/dnvm.sh" ] && . "${HOME}/.dnx/dnvm/dnvm.sh"
	echoTestProgram dnvm
	dnvmExists=$(command -v dnvm )
	if [ -z "$dnvmExists" ]
	then
		if [ ${OP_REPORT} == 1 ] 
		then
			echo "`date +"%d/%m/%Y_%H:%M:%S"` FATAL ERROR - dnvm is missing" >> ${REPORT_FILE}
			REPORT_FLAG=1
		fi
		echoErrorPkg
		if [ ${OP_INSTALL_DNX} == 1 ]
		then
			printf "${CYAN}Installing dnvm:${NC}\n"
			curl -sSL https://raw.githubusercontent.com/aspnet/Home/dev/dnvminstall.sh | DNX_BRANCH=dev sh && source ~/.dnx/dnvm/dnvm.sh
			status=$?
			if [ ${status} -eq 0 ]
			then
				printf "\n${NC}[ ${GREEN}OK${NC}  ] Install dnvm...\n\n"
			else
				printf "\n${NC}[${RED}ERROR${NC}] Install dnvm... ${RED}curl rc=${ERR}${NC}\n"
				if [ ${OP_SKIP_ERRORS} == 0 ]
				then
					exit 102
				fi
				echoSkippingErrors
			fi
		else
			if [ ${OP_SKIP_ERRORS} == "0" ]
			then
				exit 103
			fi
			echoSkippingErros
		fi
	else
		echoOkPkg
	fi

	echoExecProgram "dnvm install ${CLR_VER} -r ${CLR_RUNTIME} -arch ${CLR_ARCH} &> ${OUT_FILE}" 
	dnvm install ${CLR_VER} -r ${CLR_RUNTIME} -arch ${CLR_ARCH} &> ${OUT_FILE}
	echoSuccessExec
	echoExecProgram "dnvm use ${CLR_VER} -r ${CLR_RUNTIME} -arch ${CLR_ARCH} &>> ${OUT_FILE}" 
	dnvm use ${CLR_VER} -r ${CLR_RUNTIME} -arch ${CLR_ARCH} &>> ${OUT_FILE}
	if [ ${OP_REPORT} == 1 ] 
	then 
		echo "`date +"%d/%m/%Y_%H:%M:%S"` Using ${CLR_VER} with runtime ${CLR_RUNTIME} arch ${CLR_ARCH}" >> ${REPORT_FILE}
	fi
	status=$?
	if [ ${status} -eq 0 ]
	then
		echoSuccessExec
	else
		if [ ${OP_REPORT} == 1 ] 
		then
			echo "`date +"%d/%m/%Y_%H:%M:%S"` FATAL ERROR - Cannot use ${CLR_VER} with runtime ${CLR_RUNTIME} arch ${CLR_ARCH}. rc=${status}" >> ${REPORT_FILE}
			REPORT_FLAG=1
		fi
		echoFailExec ${status}
		if [ ${OP_SKIP_ERRORS} == 0 ]
		then
			exit 104
		fi
		echoSkippingErros
	fi

	echoTestPkg "libuv (for kestrel)"
	if [ -f /usr/local/lib/libuv.a ]
	then
		echoOkPkg
	else
		if [ ${OP_REPORT} == 1 ] 
		then
			echo "`date +"%d/%m/%Y_%H:%M:%S"` FATAL ERROR - libuv is missing" >> ${REPORT_FILE}
			REPORT_FLAG=1		
		fi
		echoErrorPkg		
		if [ ${OP_INSTALL_DNX} == 1 ]
		then
			printf "${CYAN}Installing libuv (for kestrel):${NC}\n"
			printf "(about to 'sudo' ${PKG_INSTALLER}... enter password to allow installation)\n"
			sudo ${PKG_INSTALLER} -y install make automake libtool curl 
			curl -sSL https://github.com/libuv/libuv/archive/v1.8.0.tar.gz | sudo tar zxfv - -C /usr/local/src
			cd /usr/local/src/libuv-1.8.0
			sudo sh autogen.sh
			sudo ./configure
			sudo make
			sudo make install
			sudo ldconfig
			cd -
			printf "\nRechecking libuv installation:\n"
                        echoTestPkg "libuv (for kestrel)"
                        if [ -f /usr/local/lib/libuv.a ]
                        then
                                echoOkPkg
                        else
                                echoErrorPkg
                                if [ ${OP_SKIP_ERRORS} == 0 ]
                                then
                                        exit 105
                                fi
                                echoSkippingErros
                        fi
                else
                        if [ ${OP_SKIP_ERRORS} == 0 ]
                        then
                                exit 106
                        fi
                        echoSkippingErros
                fi
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
	printf " ${RED} Not installed!${NC}"
	tput cub 9999
	tput cuf 1
	printf "${RED}ERROR${NC}\n"
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

function echoSkippingErros () {
	printf "${YELLOW} --skip-errors set, ignoring failures${NC}\n"
	# sleep 1
}

function buildRaven () {
	printf "\n${BLUE}Restoring Packages:${NC}\n"
	if [ ${OP_REPORT} == 1 ]
	then
		printf "${PURPLE}dnu restore out saved into ${OUT_FILE}.dnurestore${NC}\n"
		dnu restore >& ${OUT_FILE}.dnurestore
        	status=$?
	else
		dnu restore
		status=$?
	fi
	if [ ${status} -ne 0 ]
	then
		if [ ${OP_REPORT} == 1 ] 
		then
			echo "`date +"%d/%m/%Y_%H:%M:%S"` FATAL ERROR - Failed to restore packages. rc=${status}" >> ${REPORT_FILE}
			REPORT_FLAG=1
		fi
		printf "${NC}\n${RED}Errors in restore packages!${NC}\n"
		if [ ${OP_SKIP_ERRORS} == 0 ]
		then
			exit 107
		fi
		echoSkippingErros
	fi
		
	for i in "${BUILD_DIRS[@]}"
	do
		extra_build_args=""
		if [ ${i} == "src/Raven.Studio" ]
		then
			extra_build_args="--framework dnxcore50"
		fi
		printf "\n${BLUE}Building ${i}:${NC}\n"
		if [ ${OP_REPORT} == 1 ] 
		then
			echo -n "`date +"%d/%m/%Y_%H:%M:%S"` Build start for ${i} ... " >> ${REPORT_FILE}
		fi
		pushd ${i}
		if [ ${OP_REPORT} == 1 ] 
		then
			printf "${PURPLE}Build out saved into ${OUT_FILE}.build${NC}\n"
			dnu build ${extra_build_args} >& ${OUT_FILE}.build
			status=$?
		else
			dnu build ${extra_build_args}
			status=$?
		fi
		popd
		if [ ${status} -ne 0 ]
		then
			if [ ${OP_REPORT} == 1 ]
			then
				echo "FAILED !!" >> ${REPORT_FILE}
				echo "`date +"%d/%m/%Y_%H:%M:%S"` ERRORS:" >> ${REPORT_FILE}
				grep "error CS" ${OUT_FILE}.build >> ${REPORT_FILE}
				REPORT_FLAG=1
				OP_SKIP_TESTS=1
			fi
			printf "${NC}\n${RED}Build errors in package ${i}${NC}\n"
			if [ ${OP_SKIP_ERRORS} == 0 ]
			then
				exit 109
			fi
			echoSkippingErros
		else
			if [ ${OP_REPORT} == 1 ] 
			then
				echo "Successs." >> ${REPORT_FILE}
			fi
		fi		
	done
}

function runTests () {	
if [ ${OP_SKIP_TESTS} == 0 ]
then
	for i in "${TEST_DIRS[@]}"
	do
		if [ ${i} == "test/Tryouts" ]; then continue; fi
		printf "\n${BLUE}Testing ${i}:${NC}\n"
		if [ ${OP_REPORT} == 1 ] 
		then
			echo -n "`date +"%d/%m/%Y_%H:%M:%S"` Test start for ${i} ... " >> ${REPORT_FILE}
		fi
		pushd ${i}
		if [ ${OP_REPORT} == 1 ]
		then
			dnx test -verbose >& ${OUT_FILE}.test
                	status=$?
		else
			dnx test -verbose
			status=$?
		fi
		popd
		finline=`cat ${OUT_FILE}.test | egrep "Total.*Errors.*Failed.*Skipped" | wc -l | cut -f1`
		if [ ${finline} == 0 ] # tests can return 0 after specific errors
		then
			status=1
		fi
		if [ ${status} -ne 0 ]
		then
			if [ ${OP_REPORT} == 1 ]
                        then
				filenameToSave="${OUT_FILE}.`date +"%d%m%Y_%H%M%S"`"
                                echo "FAILED !! (output saved in ${filenameToSave})" >> ${REPORT_FILE}
				ATTACHMENTS+=("${filenameToSave}")
                                echo "`date +"%d/%m/%Y_%H:%M:%S"` ERRORS:" >> ${REPORT_FILE}
                                grep '\[FAIL\]' ${OUT_FILE}.test >> ${REPORT_FILE}
				cat ${OUT_FILE}.test | egrep "Total.*Errors.*Failed.*Skipped" >> ${REPORT_FILE}
				cat ${OUT_FILE}.test | egrep 'Aborted.*core.*dumped' >> ${REPORT_FILE}
				echo "`date +"%d/%m/%Y_%H:%M:%S"` Saving output at ${filenameToSave}"
				cp ${OUT_FILE}.test ${filenameToSave}
				REPORT_ATTACH=( ${REPORT_ATTACH} ${filenameToSave} ) 
				REPORT_FLAG=1
                        fi
			printf "${NC}\n${RED}Build errors in package ${i}${NC}\n"
			if [ ${OP_SKIP_ERRORS} == 0 ]
			then
				exit 109
			fi
			echoSkippingErros
		else
			if [ ${OP_REPORT} == 1 ] 
			then
				echo "Successs." >> ${REPORT_FILE}
				cat ${OUT_FILE}.test | egrep "Total.*Errors.*Failed.*Skipped" >> ${REPORT_FILE}

			fi
		fi		
	done
else
	printf "${YELLOW}--skip-tests is set - skipping tests (because of build fail in report mode or user specific request)${NC}\n"
fi
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

printWelcome

checkPackages

checkDnx

buildRaven

runTests

sendMail

printf "${NC}\n${GREEN}Done. Enjoy RavenDB :)${NC}\n"

exit 0



