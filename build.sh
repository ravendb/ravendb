#!/bin/bash

PKG_INSTALLER="apt-get" # supports : sudo ${PKG_INSTALLER} install <pkgname>   (apt-get and yum currently supported)

CLR_VER="1.0.0-rc1-update1"
CLR_RUNTIME="coreclr"
CLR_ARCH="x64"

TEST_DIRS=( "test/Voron.Tests" "test/BlittableTests" )
BUILD_DIRS=( "src/Voron" "src/Sparrow" "src/Raven.Client" "src/Raven.Server" )
CHK_PKGS=( "unzip" "curl" "libunwind8" "gettext" "libssl-dev" "libcurl4-openssl-dev" "zlib1g" "libicu-dev" "uuid-dev" )


OP_INSTALL_PKGS=0
OP_INSTALL_DNX=0
OP_SKIP_ERRORS=0

NC='\033[0m'
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
PURPLE='\033[0;35m'
CYAN='\033[0;36m'
BLUE='\033[0;34m'

function printWelcome () {
	printf "\n\n${CYAN}RavenDB (Linux) Build Script${BLUE} (v0.1) ${NC}\n"
	printf "${PURPLE}============================${NC}\n"
}

function printHelp () {
	printWelcome
	printf "\nUsage : build.sh [options]\n"
	printf "  Options:\n"
	printf "           --install-pkgs          : if found missing packages - try to install using packager installer\n"
	printf "           --install-dnx           : try to install dnvm and compile libuv if missing\n"
	printf "           --skip-errors           : do not exit on missing items, installation fails, build and test failures\n"
	printf "           --clr-version=<version> : set clr version to install and use (default : ${CLR_VER})\n"
	printf "           --clr-runtime=<runtime> : set clr runtime to install and use (default : ${CLR_RUNTIME})\n"
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
		--help|-h)
			printHelp
			exit 0
			;;
	esac
done


RECURSIVE_CALL=0
function checkPackages () {
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
	[ -s "/home/adi/.dnx/dnvm/dnvm.sh" ] && . "/home/adi/.dnx/dnvm/dnvm.sh"
	echoTestProgram dnvm
	dnvmExists=$(command -v dnvm )
	if [ -z "$dnvmExists" ]
	then
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

	echoExecProgram "dnvm install ${CLR_VER} -r ${CLR_RUNTIME} -arch ${CLR_ARCH} &> /tmp/build.out" 
	dnvm install ${CLR_VER} -r ${CLR_RUNTIME} -arch ${CLR_ARCH} &> /tmp/build.out
	echoSuccessExec
	echoExecProgram "dnvm use ${CLR_VER} -r ${CLR_RUNTIME} -arch ${CLR_ARCH} &>> /tmp/build.out" 
	dnvm use ${CLR_VER} -r ${CLR_RUNTIME} -arch ${CLR_ARCH} &>> /tmp/build.out
	status=$?
	if [ ${status} -eq 0 ]
	then
		echoSuccessExec
	else
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
			fi
			if [ ${OP_SKIP_ERRORS} == 0 ]
			then
				exit 105
			fi
			echoSkippingErros
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
	printf "${YELLOW} --skip-errors set, ignoring dnvm installation failure${NC}\n"
	# sleep 1
}

function buildRaven () {
	printf "\n${BLUE}Restoring Packages:${NC}\n"
	dnu restore
	status=$?
	if [ ${status} -ne 0 ]
	then
		printf "${NC}\n${RED}Errors in restore packages!${NC}\n"
		if [ ${OP_SKIP_ERRORS} == 0 ]
		then
			exit 107
		fi
		echoSkippingErros
	fi
		
	for i in "${BUILD_DIRS[@]}"
	do
		printf "\n${BLUE}Building ${i}:${NC}\n"
		pushd ${i}
		dnu build
		status=$?
		popd
		if [ ${status} -ne 0 ]
		then
			printf "${NC}\n${RED}Build errors in package ${i}${NC}\n"
			if [ ${OP_SKIP_ERRORS} == 0 ]
			then
				exit 109
			fi
			echoSkippingErros
		fi		
	done
}

function runTests () {	
	for i in "${TEST_DIRS[@]}"
	do
		printf "\n${BLUE}Testing ${i}:${NC}\n"
		pushd ${i}
		dnx test -verbose
		status=$?
		popd
		if [ ${status} -ne 0 ]
		then
			printf "${NC}\n${RED}Build errors in package ${i}${NC}\n"
			if [ ${OP_SKIP_ERRORS} == 0 ]
			then
				exit 109
			fi
			echoSkippingErros
		fi		
	done
}

printWelcome

checkPackages

# installNeeded $?

checkDnx

buildRaven

runTests

printf "${NC}\n${GREEN}Done. Enjoy RavenDB :)${NC}\n"

exit 0



