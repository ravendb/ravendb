CC = c89 # C compiler
CFLAGS = -Iinc -O2 -Wall
LDFLAGS = -shared
RM = rm -f  # rm command
ADDITIONAL_FLAGS = -MM
SRC_POSIX = $(wildcard src/posix/*.c)

ifeq ($(OS),Windows_NT)
	SRC_POSIX=""
	ifeq ($(PROCESSOR_ARCHITECTURE),AMD64)
		OSFLAG=.win.x64.dll
		OSDIR=win
	endif
	ifeq ($(PROCESSOR_ARCHITECTURE),x86)
		OSFLAG=.win.x86.dll 
		OSDIR=win
	endif
else
	UNAME_S := $(shell uname -s)
	UNAME_P := $(shell uname -p)
	UNAME_M := $(shell uname -m)
	ifeq ($(UNAME_S),Linux)
		OSDIR=posix
		ifeq ($(UNAME_P),x86_64)
			OSFLAG=.linux.x64.so
		endif
		ifneq ($(filter %86,$(UNAME_P)),)	
			OSFLAG=.linux.x86.so
		endif
	else ifeq ($(UNAME_S),Darwin)
		OSDIR=mac
		BASH32BIT := $(shell file /bin/bash | grep 32-bit | wc -l)
                ifeq ($(BASH32BIT),1)
                        OSFLAG=.mac.x86.so
                else
                        OSFLAG=.mac.x64.so
                endif
		ADDITIONAL_FLAGS = -c -std=gnu89 -fvisibility=default
		CC = clang
		LDFLAGS = -dynamiclib
	else ifneq ($(filter arm%,$(UNAME_M)),)
		OSDIR=posix	
		BASH32BIT := $(shell file /bin/bash | grep 32-bit | wc -l)
		ifeq ($(BASH32BIT),1)
			OSFLAG=.arm.32.so 
		else
			OSFLAG=.arm.64.so
		endif
	endif
endif

TARGET_LIB = librvnpal$(OSFLAG) # target lib

# invoke libsodium make 
SODIUM="/home/avivi/Downloads/libsodium-1.0.16/src/libsodium/.libs/libsodium.a"

SRC = src/$(OSDIR)
SRC_ALL = src/all
SRCS = $(wildcard $(SRC)/*.c) $(wildcard $(SRC_ALL)/*.c) $(SRC_POSIX) # source files
OBJS = $(SRCS:.c=.o)

.PHONY: all
all: ${TARGET_LIB}

$(TARGET_LIB): $(OBJS)
	$(CC) ${LDFLAGS}  -o $@ $^

$(SRCS:.c=.d):%.d:%.c
	$(CC) $(CFLAGS) $(ADDITIONAL_FLAGS)  $< >$@

# include $(SRCS:.c=.d)

.PHONY: clean
clean:
	-${RM} ${TARGET_LIB} ${OBJS} $(SRCS:.c=.d)

