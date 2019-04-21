#!/bin/bash
echo ""
LIBSODIUM_VER=1.0.17
SODIUM_LOG=${PWD}/build_sodium.log
LIBFILE="librvnpal"
CLEAN=0
C_COMPILER=c89
C_ADDITIONAL_FLAGS=""
C_SHARED_FLAG="-shared"
IS_CROSS=0
SKIP_COPY=0

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

IS_LINUX=0
IS_ARM=0
IS_MAC=0
IS_32BIT=0

function reset_before_build_libsodium {
	unset CC
	unset CFLAGS
	unset PREFIX
	unset OSX_VERSION_MIN
	unset OSX_CPU_ARCH
	unset LDFLAGS

	if [ ! -d libsodium ]; then
		echo -ne "${C_YELLOW}[`date`] ${C_L_MAGENTA}Cloning libsodium... ${NC}"
		git clone https://github.com/jedisct1/libsodium >> ${SODIUM_LOG} 2>&1
		if [ $? -ne 0 ]; then
				echo -e "${ERR_STRING}Failed to clone libsodium. This is fatal error for the build process${NC}"
				exit 1
		fi		
		echo -e "${C_GREEN}ok."
		pushd libsodium >> ${SODIUM_LOG} 2>&1
		if [ $? -ne 0 ]; then echo -e "${ERR_STRING}Failed to pushd libsodium. See ${SODIUM_LOG} for details.${NC}"; popd; exit 1; fi
		echo -ne "${C_YELLOW}[`date`] ${C_L_MAGENTA}Switching to tag ${C_L_CYAN}${LIBSODIUM_VER}${C_L_MAGENTA}... ${NC}"
		git checkout tags/${LIBSODIUM_VER} >> ${SODIUM_LOG} 2>&1
		if [ $? -ne 0 ]; then echo -e "${ERR_STRING}Failed to checkout tags/${LIBSODIUM_VER}. See ${SODIUM_LOG} for details.${NC}"; popd; exit 1; fi
		echo -e "${C_GREEN}ok."
		popd >> ${SODIUM_LOG} 2>&1
	fi
	if [ ! -d artifacts ]; then
			mkdir artifacts
	fi

	pushd libsodium >> ${SODIUM_LOG} 2>&1

	echo -ne "${C_YELLOW}[`date`] ${C_L_MAGENTA}Cleaning and autogen... "
	git clean -fxd >> ${SODIUM_LOG} 2>&1
	if [ $? -ne 0 ]; then echo -e "${ERR_STRING}Failed clean -fxd. See ${SODIUM_LOG} for details.${NC}"; popd; exit 1; fi
	./autogen.sh >> ${SODIUM_LOG} 2>&1
	if [ $? -ne 0 ]; then echo -e "${ERR_STRING}Failed ./autogen.sh. See ${SODIUM_LOG} for details.${NC}"; popd; exit 1; fi
	echo -e "${C_GREEN}ok."	
}

function build_libsodium_arm_and_arm64 {
	SUCCESS=0
	
	declare -A platArray
	platArray["aarch64-unknown-linux-gnu"]="aarch64-linux-gnu-gcc"
	platArray["arm-linux-gnueabihf"]="arm-linux-gnueabihf-gcc"
	declare -A forfileArray
	forfileArray["aarch64-unknown-linux-gnu"]="arm.64"
	forfileArray["arm-linux-gnueabihf"]="arm.32"	

	for plat in "${!platArray[@]}"; do
		reset_before_build_libsodium
		
		export CFLAGS='-Os'
		export CC=${platArray[$plat]} 
		
		echo -ne "${C_YELLOW}[`date`] ${C_L_MAGENTA}Building for ${C_L_BLUE}${forfileArray[${plat}]}${C_L_MAGENTA}... "
		
		./configure --host=${plat} --prefix=${PWD}/install_path >> ${SODIUM_LOG} 2>&1
		if [ $? -ne 0 ]; then
				echo -e "${ERR_STRING}Failed to CC=${platArray[$plat]} ./configure --host=${plat}${NC}"
		else					
			make clean >> ${SODIUM_LOG} 2>&1 && \
					make >> ${SODIUM_LOG} 2>&1 && \
					make install >> ${SODIUM_LOG} 2>&1 && \
					cp install_path/lib/libsodium.so ../artifacts/libsodium.${forfileArray[${plat}]}.so >> ${SODIUM_LOG} 2>&1
			if [ $? -ne 0 ]; then 
				echo -e "${ERR_STRING}Failed. See ${SODIUM_LOG} for details.${NC}";
			else
				if [[ "${plat}" == "aarch64-unknown-linux-gnu" ]]; then SUCCESS=$(expr ${SUCCESS} + 1); fi
				if [[ "${plat}" == "arm-linux-gnueabihf" ]]; then SUCCESS=$(expr ${SUCCESS} + 2); fi
				echo -e "${C_GREEN}ok."
			fi
		fi

		popd >> ${SODIUM_LOG} 2>&1
	done

 	# SUCCESS is : 1 for 64, 2 for 32, 3 for both	
	RC=${SUCCESS}
}

function build_libsodium_win32_and_win64 {
	SUCCESS=0

	for winbits in 32 64; do
		reset_before_build_libsodium
		
		sed -i 's/make check/echo skip check/g' ./dist-build/msys2-win${winbits}.sh 
		echo -ne "${C_YELLOW}[`date`] ${C_L_MAGENTA}Building for ${C_L_BLUE}win.${winbits}${C_L_MAGENTA}... "
		./dist-build/msys2-win${winbits}.sh >> ${SODIUM_LOG} 2>&1 && \
			cp libsodium-win${winbits}/bin/libsodium*.dll ../artifacts/libsodium.win.x${winbits}.dll >> ${SODIUM_LOG} 2>&1
		if [ $? -eq 0 ]; then
			echo -e "${C_GREEN}ok."
			if [[ "${winbits}" == "64" ]]; then SUCCESS=$(expr ${SUCCESS} + 1); fi
			if [[ "${winbits}" == "32" ]]; then SUCCESS=$(expr ${SUCCESS} + 2); fi
		else
			echo -e "${ERR_STRING}Failed. See ${SODIUM_LOG} for details.${NC}";
		fi

		popd >> ${SODIUM_LOG} 2>&1
	done

	# SUCCESS is : 1 for x64, 2 for x86, 3 for both	
	RC=${SUCCESS}
}

function build_libsodium_osx64 {
	SUCCESS=0
	reset_before_build_libsodium

	export CC="${PWD}/../osxcross/target/bin/o64-clang"
	export PREFIX="$(pwd)/libsodium-osx"
	export OSX_VERSION_MIN=${OSX_VERSION_MIN-"10.8"}
	export OSX_CPU_ARCH=${OSX_CPU_ARCH-"core2"}
	export CFLAGS="-arch x86_64 -mmacosx-version-min=${OSX_VERSION_MIN} -march=${OSX_CPU_ARCH} -O2 -g"
	export LDFLAGS="-arch x86_64 -mmacosx-version-min=${OSX_VERSION_MIN} -march=${OSX_CPU_ARCH}"

	mkdir -p $PREFIX >> ${SODIUM_LOG} 2>&1
	make distclean >> ${SODIUM_LOG} 2>&1
	if [ -z "$LIBSODIUM_FULL_BUILD" ]; then
		export LIBSODIUM_ENABLE_MINIMAL_FLAG="--enable-minimal"
	else
		export LIBSODIUM_ENABLE_MINIMAL_FLAG=""
	fi

	echo -ne "${C_YELLOW}[`date`] ${C_L_MAGENTA}Building for ${C_L_BLUE}osx-x64${C_L_MAGENTA}... "
	
	./configure ${LIBSODIUM_ENABLE_MINIMAL_FLAG} \
				--build=x86_64-unknown-linux-gnu --host=x86_64-apple-darwin --target=x86_64-apple-darwin \
				--prefix="${PREFIX}" >> ${SODIUM_LOG} 2>&1

	if [ $? -ne 0 ]; then
		echo -e "${ERR_STRING}Failed to configure ${LIBSODIUM_ENABLE_MINIMAL_FLAG} --build=x86_64-unknown-linux-gnu --host=x86_64-apple-darwin --target=x86_64-apple-darwin --prefix=${PREFIX}"
	else
		NPROCESSORS=$(getconf NPROCESSORS_ONLN 2>/dev/null || getconf _NPROCESSORS_ONLN 2>/dev/null)
		PROCESSORS=${NPROCESSORS:-3}

		make -j${PROCESSORS} install >> ${SODIUM_LOG} 2>&1
		cp libsodium-osx/lib/libsodium.dylib ../artifacts/libsodium.osx.64.dylib
		if [ $? -eq 0 ]; then 
			echo -e "${C_GREEN}ok."
			SUCCESS=1;
		else
			echo -e "${ERR_STRING}Failed. See ${SODIUM_LOG} for details.${NC}";
		fi

		# Cleanup
		make distclean >> ${SODIUM_LOG} 2>&1		
	fi

	popd >> ${SODIUM_LOG} 2>&1
	RC=${SUCCESS}
}

function build_libsodium_local_linux_x64 {
	SUCCESS=0
	reset_before_build_libsodium

	export PREFIX=${PWD}/linux-x64-libsodium
	mkdir -p $PREFIX >> ${SODIUM_LOG} 2>&1

	echo -ne "${C_YELLOW}[`date`] ${C_L_MAGENTA}Building for ${C_L_BLUE}linux-x64${C_L_MAGENTA}... "

	./configure --prefix="${PREFIX}" >> ${SODIUM_LOG} 2>&1
	if [ $? -ne 0 ]; then
		echo -e "${ERR_STRING}Failed to ./configure --prefix=${PREFIX}${NC}"
	else		
		make >> ${SODIUM_LOG} 2>&1
		if [ $? -ne 0 ]; then 
			echo -e "${ERR_STRING}Failed. See ${SODIUM_LOG} for details.${NC}";
		else
			make install >> ${SODIUM_LOG} 2>&1
			if [ $? -ne 0 ]; then 
				echo -e "${ERR_STRING}Failed. See ${SODIUM_LOG} for details.${NC}";
			else
				echo -e "${C_GREEN}ok."
				cp ${PREFIX}/lib/libsodium.so ../artifacts/libsodium.linux.x64.so >> ${SODIUM_LOG} 2>&1
				if [ $? -eq 0 ]; then SUCCESS=1; fi
			fi
		fi
	fi
	popd >> ${SODIUM_LOG} 2>&1
	RC=${SUCCESS}
}

if [ $# -eq 1 ] && [[ "$1" == "cross-build-libsodium" ]]; then
	SUM_ARM=0
	SUM_ARM64=0
	SUM_WIN32=0
	SUM_WIN64=0
	SUM_LINUX64=0
	SUM_OSX_64=0

	echo -e "${C_L_GREEN}${T_BOLD}Build libsodium${NC}${NT}"
	echo -e "${C_D_GRAY}===============${NC}"
	echo ""
	
	RC=0

	build_libsodium_arm_and_arm64
	if [[ ${RC} -eq 1 ]]; then SUM_ARM64=1; fi
	if [[ ${RC} -eq 2 ]]; then SUM_ARM=1; fi
	if [[ ${RC} -eq 3 ]]; then SUM_ARM64=1; SUM_ARM=1; fi

	build_libsodium_win32_and_win64
	if [[ ${RC} -eq 1 ]]; then SUM_WIN64=1; fi
	if [[ ${RC} -eq 2 ]]; then SUM_WIN32=1; fi
	if [[ ${RC} -eq 3 ]]; then SUM_WIN64=1; SUM_WIN32=1; fi
	
	build_libsodium_osx64
	if [[ ${RC} -eq 1 ]]; then SUM_OSX_64=1; fi

	build_libsodium_local_linux_x64
	if [[ ${RC} -eq 1 ]]; then SUM_LINUX64=1; fi

	echo -e "${NC}"
	echo -e "${NC}"
	echo -e "============================="
	echo -e "= ${C_L_MAGENTA}libsodium version: ${C_L_GREEN}${LIBSODIUM_VER}${NC} ="
	echo -e "============================="
	echo -e ""

	if  [ ${SUM_ARM} -eq 1 ] && \
		[ ${SUM_ARM64} -eq 1 ] && \
		[ ${SUM_WIN32} -eq 1 ] && \
		[ ${SUM_WIN64} -eq 1 ] && \
		[ ${SUM_LINUX64} -eq 1 ] && \
		[ ${SUM_OSX_64} -eq 1 ]; 
	then
			echo -e "${C_L_GREEN}All platform's ${C_D_GREEN} libsodium Successfully cross compiled. Check artifacts for results.${NC}"
			echo ""
			exit 0
	fi

	declare -A sumArray
	sumArray["linux-arm"]=${SUM_ARM}
	sumArray["linux-arm64"]=${SUM_ARM64}
	sumArray["win-x86"]=${SUM_WIN32}
	sumArray["win-x64"]=${SUM_WIN64}
	sumArray["linux-x64"]=${SUM_LINUX64}
	sumArray["osx-x64"]=${SUM_OSX_64}

	for sum in "${!sumArray[@]}"; do
		if [ ${sumArray[${sum}]} -eq 1 ]; then
			echo -e "${C_L_GREEN}${sum} ${C_D_GREEN} libsodium Successfully cross compiled${NC}"
		else
			MORE_TXT=""
			if [[ "${sum}" == "osx-x64" ]]; then MORE_TXT="(make sure './build-all-posix.sh setup' was executed first)"; fi
			echo -e "${ERR_STRING}Failed to cross-compile libsodium for ${sum}${NC} ${MORE_TXT}"
		fi
	done

	echo ""	
	exit 1
fi

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
			C_COMPILER="osxcross/target/bin/o64-clang"
		        C_ADDITIONAL_FLAGS="-Wno-ignored-attributes"
		elif [[ "$2" == "linux-arm" ]]; then
                        IS_CROSS=1
                        IS_ARM=1
                        IS_32BIT=1
                        C_COMPILER=arm-linux-gnueabihf-gcc
			C_ADDITIONAL_FLAGS="-Wno-int-to-pointer-cast -Wno-pointer-to-int-cast"
		elif [[ "$2" == "linux-arm64" ]]; then
                        IS_CROSS=1
                        IS_ARM=1
                        IS_32BIT=0
                        C_COMPILER=aarch64-linux-gnu-gcc
		elif [[ "$2" == "linux-x64" ]]; then
			IS_CROSS=1
			IS_LINUX=1
		else
			echo -e "${ERR_STRING}Invalid architecture for cross compiling${NC}"
			exit 1
		fi
		if [ $# -eq 3 ]; then
			if [[ "$3" == "clean" ]]; then
				CLEAN=1
			elif [[ "$3" == "skip-copy" ]]; then
				SKIP_COPY=1
			else
				echo -e "${ERR_STRING}Invalid arguments for cross compiling${NC}"
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

echo -e "${C_L_GREEN}${T_BOLD}Build librvnpal${NC}${NT}"
echo -e "${C_D_GRAY}===============${NC}"
echo ""

IS_COMPILER=$(which ${C_COMPILER} | wc -l)

if [ ${IS_CROSS} -eq 0 ]; then
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
else
	echo -e "${C_MAGENTA}${T_UL}Cross Compilation${NC}${NT}"
        echo ""
fi

if [ ${CLEAN} -eq 0 ] && [ ${IS_COMPILER} -ne 1 ]; then
	echo -e "${ERR_STRING}Unable to determine if ${C_COMPILER} compiler exists."
        echo -e "${C_D_YELLOW}    Consider installing :"
        echo -e "                                       linux-x64 / linux-arm     - clang-3.9 and/or c89"
	echo -e "                                       cross compiling linux-arm - gcc make gcc-arm-linux-gnueabi binutils-arm-linux-gnueabi, crossbuild-essential-armhf"
	echo -e "                                       cross compiling osx-x64   - libc6-dev-i386 cmake libxml2-dev fuse clang libbz2-1.0 libbz2-dev libbz2-ocaml libbz2-ocaml-dev libfuse-dev + download Xcode_7.3, and follow https://github.com/tpoechtrager/osxcross instructions"
	echo -e "${NC}"
	exit 1
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
	FILTERS=(src/posix)
	LINKFILE=${LIBFILE}.mac
	if [ ${IS_CROSS} -eq 0 ]; then
		C_COMPILER="clang"
	        C_ADDITIONAL_FLAGS="-std=c89 -Wno-ignored-attributes"
	fi
	C_SHARED_FLAG="-dynamiclib"
	if [ ${IS_32BIT} -eq 1 ]; then
		echo -e "${ERR_STRING}mac 32 bit build is not supported${NC}"
		exit 1
        else
                LINKFILE=${LINKFILE}.x64.dylib
        fi
fi
if [ ${IS_ARM} -eq 1 ]; then
        FILTERS=(src/posix);
        LINKFILE=${LIBFILE}.arm
	if [ ${IS_32BIT} -eq 1 ]; then
                LINKFILE=${LINKFILE}.32.so
        else
                LINKFILE=${LINKFILE}.64.so
        fi
fi

if [[ "${FILTERS[0]}" == "-1" ]]; then 
	echo -e "${ERR_STRING}Not supported platform. Execute on either linux-x64, linux-arm, linux-arm64 or osx-x64${NC}"
	exit 1
fi


if [ ${CLEAN} -eq 1 ]; then
	echo -e "${C_L_CYAN}Cleaning for ${C_L_GREEN}${LINKFILE}${NC}"
fi

echo -e "${C_L_YELLOW}FILTER:   ${C_YELLOW}${FILTERS[@]}${NC}"
FILES=()
FILTERS+=(src)
for SRCPATH in "${FILTERS[@]}"; do
	for SRCFILE in $(find ${SRCPATH} -maxdepth 1 | grep "^${SRCPATH}" | grep "\.c$"); do
		SRCFILE_O=$(echo ${SRCFILE} | sed -e "s|\.c$|\.o|g")
		if [ ${CLEAN} -eq 0 ]; then
			CMD="${C_COMPILER} ${C_ADDITIONAL_FLAGS} -fPIC -Iinc -O2 -Wall -c -o ${SRCFILE_O} ${SRCFILE}"
			echo -e "${C_L_GREEN}${CMD}${NC}${NT}"
			eval ${CMD}
		else
			CMD="rm ${SRCFILE_O} >& /dev/null"
			eval ${CMD}
			STATUS=$?
			if [ ${STATUS} -eq 0 ]; then
				echo -e "${NC}[${C_GREEN} DELETED ${NC}] ${C_L_GRAY}${SRCFILE_O}${NC}${NT}"
			else
				echo -e "${NC}[${C_RED}NOT FOUND${NC}] ${C_L_GRAY}${SRCFILE_O}${NC}${NT}"
			fi
		fi
		FILES+=(${SRCFILE_O})
	done
done
if [ ${CLEAN} -eq 0 ]; then
	CMD="${C_COMPILER} ${C_SHARED_FLAG} -o ${LINKFILE} ${FILES[@]}"
	echo -e "${C_L_GREEN}${T_BOLD}${CMD}${NC}${NT}"
else
	if [ -f ${LINKFILE} ]; then 
		CMD="rm ${LINKFILE}"
		echo -e "${C_L_GREEN}${T_BOLD}${CMD}${NC}${NT}"
	else
		CMD=""
	fi
fi
${CMD}
TMP_STAT=$?
if [ ${CLEAN} -eq 0 ]; then
	if [ ${TMP_STAT} -ne 0 ]; then
		echo ""
		echo -e "${ERR_STRING}: Build Failed."
		exit 1
	fi
	echo ""
	echo -e "${C_L_CYAN}Done for ${C_L_GREEN}${T_BLINK}${LINKFILE}${NT}${NC}"
fi
echo ""
if [ ${CLEAN} -eq 0 ]; then
	if [ ${SKIP_COPY} -eq 0 ]; then
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
	else
		echo "---> ${LINKFILE}"
	fi
fi

