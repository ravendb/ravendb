#!/bin/bash
if [ "${PWD}" == "/" ]; then
	CURDIR=""
else
	CURDIR="${PWD}"
fi

PKGS=( libc6-dev libcap2-bin )
LOG=${CURDIR}/setupDockerForStacktraces.log
echo $(date) > ${LOG}
PERFORM_APT_UPGRADE=0
SUDO="sudo"
function print_usage() {
	echo "Usage: ${0} [ OPTIONS ]"
	echo "       OPTIONS:"
	echo "               --apt-upgrade                       perform apt-upgrade -y (non interactive with default confirmation)"
	echo "               --skip-sudo                         perform super user actions without sudo command"
	echo ""
}
if [ $# -gt 0 ]; then
	for arg in $@; do
		if [ ${arg} == "--apt-upgrade" ]; then
			PERFORM_APT_UPGRADE=1
		elif [ ${arg} == "--skip-sudo" ]; then
			SUDO=""
		else
			echo "Error: Invalid argument '${arg}'"
			print_usage
			exit 1
		fi
	done
fi

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

function print_state_installed() { echo -e "\r${NC}[${C_GREEN} INSTALLED ${NC}] ${1} ${C_GREEN}${2}${NT}${NC}"
} 
function print_state_error() { echo -e "\r${NC}[${C_RED} ERROR     ${NC}] ${1} ${C_RED}${2}${NT}${NC}" 
}
function print_state_msg() { echo -en "\r${NC}[${C_YELLOW}${2}${NC}] ${1}${NT}${NC}" 
}
function print_build_failed() { echo -e "${C_RED}Build Failed.${STATUS}${NC}${NT}" 
}

function progress_show_and_wait()
{
	STRFIXLEN=60
	STRHALF=$(expr ${STRFIXLEN} / 2 - 2)
	while [ $(ps -ef | awk '{print $2}' | grep "^${1}$" | wc -l) -ne 0 ]; do
		STRLINE="$(tail -1 ${LOG})"
		STRLINE="$(echo -n ${STRLINE} | tr --delete '\n')"
		STRLENG=$(echo -n $STRLINE | wc -c)
		if [ ${STRLENG} -lt ${STRFIXLEN} ]; then
			STRLINE="${STRLINE}$(printf %-$(expr 60 - ${STRLENG})s)"
		elif [ ${STRLENG} -gt ${STRFIXLEN} ]; then
			STRLINE="$(echo -n ${STRLINE} | head -c${STRHALF})....$(echo -n ${STRLINE} | tail -c${STRHALF})"
		fi
		print_state_msg "${3}${C_WHITE} <${STRLINE}>" "${2}"
		sleep 1
	done
	print_state_msg "${3}${C_L_GRAY}  $(printf %-${STRFIXLEN}s)  " "${2}"
	wait ${1}
	STATUS=$?
}

function print_welcome_and_test_sudo()
{
	echo -e "${C_L_GREEN}${T_BOLD}Setup Docker for Stacktraces in RavenDB${NC}${NT}"
	echo -e "${C_D_GRAY}=======================================${NC}"
	echo ""
	echo "Logging into file: ${LOG}"
	if [ ${PERFORM_APT_UPGRADE} -eq 1 ]; then
		echo -e "${T_BLINK}Note: 'apt upgrade' will be performed${NT}${NC}"
	else
		echo "Note: 'apt upgrade' will be skipped. Run again with --apt-upgrade to include upgrade operation."
	fi
	if [ "${SUDO}" == "" ]; then
		echo "Note: Skipping sudo command"
	else
		echo -e "${C_L_YELLOW}(Please enter sudo password, if asked to)${NC}${NT}"
	fi
	${SUDO} echo ""
}

function install_packages()
{
	MAINSTR="${C_L_CYAN}Performing ${C_L_MAGENTA}apt update${C_L_GRAY}..."
	${SUDO} /bin/bash -c "apt update" >> ${LOG} 2>&1 &
        PID=$!
	progress_show_and_wait ${PID} "Updating..." "${MAINSTR}"
	if [ ${STATUS} -eq 0 ]; then
		print_state_installed "${MAINSTR}" "Successfully updated."
	else
		print_state_error "${MAINSTR}" "Exit code ${STATUS}"
		print_build_failed
		exit 1
	fi

	if [ ${PERFORM_APT_UPGRADE} -eq 1 ]; then
		MAINSTR="${C_L_CYAN}Performing ${C_L_MAGENTA}apt upgrade -y (non interactive)${C_L_GRAY}..."
		${SUDO} /bin/bash -c "DEBIAN_FRONTEND=noninteractive apt upgrade -y" >> ${LOG} 2>&1 &
		PID=$!
		progress_show_and_wait ${PID} "Upgrading.." "${MAINSTR}"
		if [ ${STATUS} -eq 0 ]; then
			print_state_installed "${MAINSTR}" "Successfully upgraded."
		else
			print_state_error "${MAINSTR}" "Exit code ${STATUS}"
			print_build_failed
			exit 1
		fi
	fi

	HADERRORS=0
	for pkg in ${PKGS[@]}; do
		MAINSTR="${C_L_CYAN}Installing ${C_L_MAGENTA}${pkg}${C_L_GRAY}..."
		print_state_msg "${MAINSTR}" "Install ..."
		${SUDO} /bin/bash -c "DEBIAN_FRONTEND=noninteractive apt install -y ${pkg}" >> ${LOG} 2>&1
		STATUS=$?
		if [ ${STATUS} -eq 0 ]; then
			print_state_installed "${MAINSTR}" "Successfully installed."
		else
			print_state_error "${MAINSTR}" "Exit code ${STATUS}"
			HADERRORS=1
		fi
	done
	if [ ${HADERRORS} -eq 1 ]; then
		print_build_failed
		exit 1
	fi
}

function set_permissions()
{
	COMMANDS=(
		'chown root:root /opt/RavenDB/Server/Raven.Debug.dll'
	        'chmod +s /opt/RavenDB/Server/Raven.Debug.dll'
       		'setcap cap_sys_ptrace=eip /opt/RavenDB/Server/Raven.Debug.dll'
		)

	for cmd in "${COMMANDS[@]}"; do
		MAINSTR="${C_L_CYAN}Executing  ${C_L_MAGENTA}${cmd}${C_L_GRAY}..."
                print_state_msg "${MAINSTR}" "Executing.."
                ${SUDO} /bin/bash -c "eval ${cmd}" >> ${LOG} 2>&1
                STATUS=$?
                if [ ${STATUS} -eq 0 ]; then
                        print_state_installed "${MAINSTR}" "Successfully executed."
                else
                        print_state_error "${MAINSTR}" "Exit code ${STATUS}"
			print_build_failed
			exit 1
                fi
	done
}

print_welcome_and_test_sudo
install_packages
set_permissions
echo ""
echo "Done."
