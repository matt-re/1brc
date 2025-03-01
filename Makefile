CC = clang
CFLAGS = -std=c99 -O2 -Wall -Werror -Wextra -pedantic
CFLAGS += -Wshadow
CFLAGS += -Wconversion
CFLAGS += -fstrict-aliasing -Wstrict-aliasing
CFLAGS += -Wno-implicit-int-float-conversion

ifndef MAX_THREAD
	UNAME := $(shell uname)
	ifeq ($(UNAME), Darwin)
		MAX_THREAD=$(shell sysctl -n hw.logicalcpu)
	endif
	ifeq ($(UNAME), Linux)
		MAX_THREAD=$(shell nproc)
	endif
endif
ifdef MAX_THREAD
CFLAGS += -DMAX_THREAD=$(MAX_THREAD)
endif

LDFLAGS = -Wall -pedantic

all: 1brc

%.o: %.c
	$(CC) -c $(CFLAGS) $<

1brc: main.o
	$(CC) -o $@ $(LDFLAGS) $^

.PHONEY: clean
.PHONEY: run

clean:
	@rm -f 1brc *.o

run: 1brc
	./1brc

