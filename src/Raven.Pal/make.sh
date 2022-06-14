#!/bin/bash
echo ""

export LIBFILE="librvnpal"
export CLEAN=0
export C_COMPILER=c89
export C_ADDITIONAL_FLAGS=""
export C_SHARED_FLAG="-shared"
export IS_CROSS=0
export SKIP_COPY=0

IS_LINUX=0
IS_ARM=0
IS_MAC=0
IS_32BIT=0

function log {
   echo "= [$(date --iso-8601=seconds)]: $1"
}

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
elif [ $# -gt 1 ]; then
	if [[ "$1" == "cross" ]]; then
		if [[ "$2" == "osx-x64" ]]; then
			IS_CROSS=1
			IS_MAC=1
			NATIVE_BIN_SUFFIX=".mac.x64.dylib"
			C_COMPILER="osxcross/target/bin/o64-clang"
			C_ADDITIONAL_FLAGS="-Wno-ignored-attributes"
		elif [[ "$2" == "osx-arm64" ]]; then
			IS_CROSS=1
                        IS_ARM=1
			IS_MAC=1
			NATIVE_BIN_SUFFIX=".mac.arm64.dylib"
			C_COMPILER="osxcross/target/bin/oa64-clang"
			C_ADDITIONAL_FLAGS="-Wno-ignored-attributes"
		elif [[ "$2" == "linux-arm" ]]; then
                        IS_CROSS=1
                        IS_ARM=1
                        IS_32BIT=1
			NATIVE_BIN_SUFFIX=".linux.arm.32.so"
                        C_COMPILER=arm-linux-gnueabihf-gcc
			C_ADDITIONAL_FLAGS="-Wno-int-to-pointer-cast -Wno-pointer-to-int-cast"
		elif [[ "$2" == "linux-arm64" ]]; then
                        IS_CROSS=1
                        IS_ARM=1
                        IS_32BIT=0
			NATIVE_BIN_SUFFIX=".linux.arm.64.so"
                        C_COMPILER=aarch64-linux-gnu-gcc
		elif [[ "$2" == "linux-x64" ]]; then
			IS_CROSS=1
			IS_LINUX=1
			NATIVE_BIN_SUFFIX=".linux.x64.so"
		else
			log "${ERR_STRING}Invalid architecture for cross compiling${NC}"
			exit 1
		fi

		if [ $# -eq 3 ]; then
			if [[ "$3" == "clean" ]]; then
				CLEAN=1
			elif [[ "$3" == "skip-copy" ]]; then
				SKIP_COPY=1
			else
				log "${ERR_STRING}Invalid arguments for cross compiling${NC}"
				exit 1
			fi
		fi
	else
		echo -e "${ERR_STRING}Invalid arguments${NC}"
                exit 1
        fi
elif [ $# -ne 0 ]; then
	echo -e "${ERR_STRING}Invalid arguments${NC}"
        exit 1
fi

log "Build librvnpal"
log "==============="
log ""

IS_COMPILER=$(which ${C_COMPILER} | wc -l)

if [ ${IS_CROSS} -eq 0 ]; then
	log "$(uname -a)"
	log ""

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
else
	log "Cross Compilation"
        log ""
fi

if [ ${CLEAN} -eq 0 ] && [ ${IS_COMPILER} -ne 1 ]; then
	log "${ERR_STRING}Unable to determine if ${C_COMPILER} compiler exists."
	log "${C_D_YELLOW}    Consider installing :"
	log "                                       linux-x64 / linux-arm     - clang-3.9 and/or c89"
	log "                                       cross compiling linux-arm - gcc make gcc-arm-linux-gnueabi binutils-arm-linux-gnueabi, crossbuild-essential-armhf"
	log "                                       cross compiling osx-x64   - libc6-dev-i386 cmake libxml2-dev fuse clang libbz2-1.0 libbz2-dev libbz2-ocaml libbz2-ocaml-dev libfuse-dev + download Xcode_7.3, and follow https://github.com/tpoechtrager/osxcross instructions"
	log "${NC}"
	exit 1
fi
FILTERS=(-1)

FILTERS=(src/posix);
LINKFILE="${LIBFILE}${NATIVE_BIN_SUFFIX}"

if [ "${IS_MAC}" -eq 1 ]; then
	if [ ${IS_CROSS} -eq 0 ]; then
		C_COMPILER="clang"
		C_ADDITIONAL_FLAGS="-std=c89 -Wno-ignored-attributes"
	fi
	C_SHARED_FLAG="-dynamiclib"
fi

if [[ "${FILTERS[0]}" == "-1" ]]; then 
	log "${ERR_STRING}Not supported platform. Execute on either linux-x64, linux-arm, linux-arm64, macos-x64, macos-arm64"
	exit 1
fi


if [ ${CLEAN} -eq 1 ]; then
	log "${C_L_CYAN}Cleaning for ${C_L_GREEN}${LINKFILE}${NC}"
fi

log "FILTER:   ${FILTERS[@]}"
FILES=()
FILTERS+=(src)
for SRCPATH in "${FILTERS[@]}"; do
	for SRCFILE in $(find ${SRCPATH} -maxdepth 1 | grep "^${SRCPATH}" | grep "\.c$"); do
		SRCFILE_O=$(echo ${SRCFILE} | sed -e "s|\.c$|\.o|g")
		if [ ${CLEAN} -eq 0 ]; then
			CMD="${C_COMPILER} ${C_ADDITIONAL_FLAGS} -fPIC -Iinc -O2 -Wall -c -o ${SRCFILE_O} ${SRCFILE}"
			log "${C_L_GREEN}${CMD}${NC}${NT}"
			eval ${CMD}
		else
			CMD="rm ${SRCFILE_O} >& /dev/null"
			eval "${CMD}"
			STATUS=$?
			if [ ${STATUS} -eq 0 ]; then
				log "[ DELETED ] ${SRCFILE_O}"
			else
				log "[ NOT FOUND ] ${SRCFILE_O}"
			fi
		fi
		FILES+=( ${SRCFILE_O} )
	done
done
if [ ${CLEAN} -eq 0 ]; then
	CMD="${C_COMPILER} ${C_SHARED_FLAG} -o ${LINKFILE} ${FILES[*]}"
	log "${CMD}"
else
	if [ -f ${LINKFILE} ]; then 
		CMD="rm -v ${LINKFILE}"
		log "${CMD}"
	else
		CMD=""
	fi
fi

${CMD}
TMP_STAT=$?

if [ ${CLEAN} -eq 0 ]; then
	if [ ${TMP_STAT} -ne 0 ]; then
		log ""
		log "${ERR_STRING}: Build Failed."
		exit 1
	fi
	log ""
	log "Done for ${LINKFILE}"
fi

echo ""

if [ ${CLEAN} -eq 0 ]; then
	if [ ${SKIP_COPY} -eq 0 ]; then
		log "Do you want to copy ${LINKFILE} to ../../libs/librvnpal/${LINKFILE} [Y/n] ?"
		read -rn 1 -d '' "${T[@]}" "${S[@]}" ANSWER
		log ""
		log ""
		if [[ "${ANSWER}" == "" ]] || [[ "${ANSWER}" == "y" ]] || [[ "${ANSWER}" == "Y" ]]; then
			CMD="cp -v ${LINKFILE} ../../libs/librvnpal/${LINKFILE}"
			log "${CMD}"
			${CMD}
			log ""
			log "Exiting..."
		fi
	else
		echo "---> ${LINKFILE}"
	fi
fi

