CC = clang
CFLAGS = -std=c99 -O2 -Wall -Werror -Wextra -pedantic
CFLAGS += -Wshadow
CFLAGS += -Wconversion
CFLAGS += -fstrict-aliasing -Wstrict-aliasing
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

