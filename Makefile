CC = clang
CFLAGS = -std=c11 -O3 -march=native -Wall -Werror -Wextra -pedantic
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

LDFLAGS = -Wall -pedantic -pthread

all: 1brc gen1e9

%.o: %.c
	$(CC) -c $(CFLAGS) $<

1brc: main.o
	$(CC) -o $@ $(LDFLAGS) $^

gen1e9: gen1e9.c
	$(CC) $(CFLAGS) -o $@ $<

measurements1e9.txt: gen1e9
	./gen1e9 > $@

.PHONY: clean
.PHONY: run

clean:
	@rm -f 1brc gen1e9 *.o

run: 1brc
	./1brc

