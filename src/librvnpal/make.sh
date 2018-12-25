#!/bin/bash
echo ""
LIBFILE="librvnpal"
CLEAN=0
C_COMPILER=c89

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

if [ $# -eq 1 ]; then
	if [[ "$1" == "clean" ]]; then
		CLEAN=1
	elif [[ "$1" == "-h" ]] || [[ "$1" == "--help" ]]; then
		echo "Usage : make.sh [clean]"
		exit 2
	else
		echo -e "${ERR_STRING}Invalid arguments${NC}"
		exit 1
	fi
elif [ $# -ne 0 ]; then
	echo -e "${ERR_STRING}Invalid arguments${NC}"
        exit 1
fi

echo -e "${C_L_GREEN}${T_BOLD}Build librvnpal${NC}${NT}"
echo -e "${C_D_GRAY}===============${NC}"
echo ""
echo -e "${C_MAGENTA}${T_UL}$(uname -a)${NC}${NT}"
echo ""

IS_LINUX=$(uname -s | grep Linux  | wc -l)
IS_ARM=$(uname -m | grep arm    | wc -l)
IS_MAC=$(uname -s | grep Darwin | wc -l)
IS_32BIT=$(file /bin/bash | grep 32-bit | wc -l)
IS_COMPILER=$(which ${C_COMPILER} | wc -l)

if [ ${IS_LINUX} -eq 1 ] && [ ${IS_ARM} -eq 1 ]; then
	IS_LINUX=0
fi
if [ ${IS_LINUX} -eq 1 ] && [ ${IS_MAC} -eq 1 ]; then
	IS_LINUX=0
fi

if [ ${IS_COMPILER} -ne 1 ]; then
	echo -e "${ERR_STRING}Unable to determine if ${C_COMPILER} compiler exists. Consider installing clang-3.9"
	return 1
fi
FILTERS=(-1)
if [ ${IS_LINUX} -eq 1 ]; then 
	FILTERS=(src/posix);
	LINKFILE=${LIBFILE}.linux
	if [ ${IS_32BIT} -eq 1 ]; then
		echo -e "${ERR_STRING}linux x86 bit build is not supported${NC}"
                exit 1
	else
		LINKFILE=${LINKFILE}.x64.so
	fi
fi
if [ ${IS_MAC} -eq 1 ]; then
	FILTERS=(src/posix src/mac);
	LINKFILE=${LIBFILE}.mac
	if [ ${IS_32BIT} ]; then
		echo -e "${ERR_STRING}mac 32 bit build is not supported${NC}"
		exit 1
        else
                LINKFILE=${LINKFILE}.x64.dylib
        fi
fi
if [ ${IS_ARM} -eq 1 ]; then
        FILTERS=(src/posix);
        LINKFILE=${LIBFILE}.arm
	if [ ${IS_32BIT} ]; then
                LINKFILE=${LINKFILE}.32.so
        else
		echo -e "${ERR_STRING}arm 64 bit build is not supported${NC}"
                exit 1
        fi
fi

if [[ "${FILTERS[0]}" == "-1" ]]; then 
	echo -e "${ERR_STRING}Not supported platform. Execute on either linux-x64, arm-32 or osx-x64${NC}"
	exit 1
fi


if [ ${CLEAN} -eq 1 ]; then
	echo -e "${C_L_CYAN}Cleaning for ${C_L_GREEN}${LINKFILE}${NC}"
fi

echo -e "${C_L_YELLOW}FILTER:   ${C_YELLOW}${FILTERS}${NC}"
FILES=()
for SRCPATH in "${FILTERS[@]}"; do
	for SRCFILE in $(find ${SRCPATH} | grep "^${FILTERS}" | grep "\.c$"); do
		SRCFILE_O=$(echo ${SRCFILE} | sed -e "s|\.c$|\.o|g")
		if [ ${CLEAN} -eq 0 ]; then
			CMD="${C_COMPILER} -Iinc -O2 -Wall -c -o ${SRCFILE_O} ${SRCFILE}"
		else
			if [ -f ${LINKFILE} ]; then 
				CMD="rm ${SRCFILE_O}"
			else
				CMD=""
				continue
			fi
		fi
		echo -e "${C_L_GREEN}${CMD}${NC}${NT}"
		${CMD}
		FILES+=(${SRCFILE_O})
	done
done
if [ ${CLEAN} -eq 0 ]; then
	CMD="${C_COMPILER} -shared -o ${LINKFILE} ${FILES[@]}"
	echo -e "${C_L_GREEN}${T_BOLD}${CMD}${NC}${NT}"
else
	if [ -f ${LINKFILE} ]; then 
		CMD="rm ${LINKFILE}"
		echo -e "${C_L_GREEN}${T_BOLD}${CMD}${NC}${NT}"
	fi
fi
${CMD} 
if [ $? -ne 0 ]; then
	echo ""
	echo -e "${ERR_STRING}: Build Failed."
	return 1
fi
echo ""
echo -e "${C_L_CYAN}Done for ${C_L_GREEN}${T_BLINK}${LINKFILE}${NT}${NC}"
if [ ${CLEAN} -eq 0 ]; then
	echo ""
	echo -ne "${C_L_RED}Do you want to copy ${C_L_GREEN}${LINKFILE}${C_L_RED} to ${C_L_GREEN}../../libs/librvnpal/${LINKFILE}${C_RED} [Y/n] ? ${NC}"
	read -rn 1 -d '' "${T[@]}" "${S[@]}" ANSWER
	echo ""
	echo ""
	if [[ "${ANSWER}" == "" ]] || [[ "${ANSWER}" == "y" ]] || [[ "${ANSWER}" == "Y" ]]; then
		CMD="cp ${LINKFILE} ../../libs/librvnpal/${LINKFILE}"
		echo -e "${C_L_GREEN}${T_BOLD}${CMD}${NC}${NT}"
		${CMD}
		echo ""
		echo "Exiting..."
	fi
fi

