#!/bin/bash

export LIBSODIUM_VER=1.0.17
export SODIUM_LOG=${PWD}/build_sodium.log
export LOG="$SODIUM_LOG"

function install_build_deps_libsodium {
    echo -e "${C_YELLOW}[`date`] ${C_L_MAGENTA}Install libsodium build dependencies... ${NC}"

    sudo apt install -y libtool autoconf gcc-mingw-w64

    if [ $? -ne 0 ]; then
        echo "Failed to install libsodium build dependencies."
        exit 1
    fi
}

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

function build_libsodium_osx_arm64 {
	SUCCESS=0
	reset_before_build_libsodium

	export CC="${PWD}/../osxcross/target/bin/oa64-clang"
	export PREFIX="$(pwd)/libsodium-osx-arm64"
	export OSX_VERSION_MIN=${OSX_VERSION_MIN-"10.8"}
	export OSX_CPU_ARCH=${OSX_CPU_ARCH-"core2"}
	export CFLAGS="-arch arm64 -mmacosx-version-min=${OSX_VERSION_MIN} -O2 -g"
	export LDFLAGS="-arch arm64 -mmacosx-version-min=${OSX_VERSION_MIN} "

	mkdir -p $PREFIX >> ${SODIUM_LOG} 2>&1
	make distclean >> ${SODIUM_LOG} 2>&1
	if [ -z "$LIBSODIUM_FULL_BUILD" ]; then
		export LIBSODIUM_ENABLE_MINIMAL_FLAG="--enable-minimal"
	else
		export LIBSODIUM_ENABLE_MINIMAL_FLAG=""
	fi

	echo -ne "${C_YELLOW}[`date`] ${C_L_MAGENTA}Building for ${C_L_BLUE}osx-arm64${C_L_MAGENTA}... "
	
	./configure ${LIBSODIUM_ENABLE_MINIMAL_FLAG} \
				--build=aarch64-unknown-linux-gnu --host=aarch64-apple-darwin --target=aarch64-apple-darwin \
				--prefix="${PREFIX}" >> ${SODIUM_LOG} 2>&1

	if [ $? -ne 0 ]; then
		echo -e "${ERR_STRING}Failed to configure."
	else
		NPROCESSORS=$(getconf NPROCESSORS_ONLN 2>/dev/null || getconf _NPROCESSORS_ONLN 2>/dev/null)
		PROCESSORS=${NPROCESSORS:-3}

		make -j${PROCESSORS} install >> ${SODIUM_LOG} 2>&1
		cp libsodium-osx-arm64/lib/libsodium.dylib ../artifacts/libsodium.mac.arm64.dylib
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

function libsodium_cross_build () {
	SUM_ARM=0
	SUM_ARM64=0
	SUM_WIN32=0
	SUM_WIN64=0
	SUM_LINUX64=0
	SUM_OSX_64=0
	SUM_OSX_ARM64=0

	echo -e "${C_L_GREEN}${T_BOLD}Build libsodium${NC}${NT}"
	echo -e "${C_D_GRAY}===============${NC}"
	echo ""
	
	RC=0

    install_build_deps_libsodium

	# build_libsodium_osx64
	# if [[ ${RC} -eq 1 ]]; then SUM_OSX_64=1; fi

	build_libsodium_osx_arm64
	if [[ ${RC} -eq 1 ]]; then SUM_OSX_ARM64=1; fi
exit 1
	build_libsodium_arm_and_arm64
	if [[ ${RC} -eq 1 ]]; then SUM_ARM64=1; fi
	if [[ ${RC} -eq 2 ]]; then SUM_ARM=1; fi
	if [[ ${RC} -eq 3 ]]; then SUM_ARM64=1; SUM_ARM=1; fi

	build_libsodium_win32_and_win64
	if [[ ${RC} -eq 1 ]]; then SUM_WIN64=1; fi
	if [[ ${RC} -eq 2 ]]; then SUM_WIN32=1; fi
	if [[ ${RC} -eq 3 ]]; then SUM_WIN64=1; SUM_WIN32=1; fi
	

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
		[ ${SUM_OSX_ARM64} -eq 1 ] && \
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
	sumArray["osx-arm64"]=${SUM_OSX_ARM64}

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
}
