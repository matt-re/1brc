#include <limits.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define MAX_THREAD		8
#define MAX_LINE_LEN		107
#define DIV_ROUND_UP(a, n)	(((a) + (n) - 1) / (n))
#define ROUND_UP_LINE(a)	(DIV_ROUND_UP((a), MAX_LINE_LEN) * MAX_LINE_LEN)
#define MAX_CAPACITY		16384
#define FNV1A_OFFSET		UINT64_C(14695981039346656037)
#define FNV1A_PRIME		UINT64_C(1099511628211)

struct station
{
	int max;
	int min;
	int sum;
	int cnt;
	unsigned int nname;
	char name[100];
	char pad[8];
};

struct station stations[MAX_CAPACITY];

static struct station *
get_station(char *name, unsigned int nname, unsigned long long hash)
{
	unsigned long long i = hash & (MAX_CAPACITY - 1);
	for (;;) {
		if (!stations[i].cnt) {
			stations[i].cnt = 0;
			stations[i].max = INT_MIN;
			stations[i].min = INT_MAX;
			stations[i].sum = 0;
			memcpy(stations[i].name, name, nname);
			stations[i].nname = nname;
			return &stations[i];
		}
		if (stations[i].nname == nname && memcmp(stations[i].name, name, nname) == 0) {
			return &stations[i];
		}
		i = (i + 1) % MAX_CAPACITY;
	}
}

static void
read_lines(char *buf, size_t nbuf, size_t rest, FILE *stream)
{
	/* Each batch after first needs to contain the characters from the
	 * previous batch to handle a line being split across batches. The
	 * current batch will only read up to the last \n character, which
	 * means some characters in the current batch will be ignored. The next
	 * batch will read characters from the end of the previous batch, at
	 * most one extea whole line.
	 */
	size_t nread = fread(buf, 1, nbuf, stream);
	if (nread == 0) return;
	fseek(stream, -MAX_LINE_LEN, SEEK_CUR); 
	/* When in batch [1,N) need to start from a partial line from the
	 * previous batch.
	 */
	char *beg;
	if (rest) {
		beg = buf + MAX_LINE_LEN;
		while (*--beg !='\n');
		++beg;
	} else {
		beg = buf;
	}
	char *end = buf + nread;
	while (*--end != '\n');

	char *cur = beg;
	while (cur < end) {
		char *name = cur;
		while (*cur++ != ';');
		int nname = (int)(cur - name) - 1;
		char *num = cur;
		while (*cur++ != '\n');
		int nnum = (int)(cur - num) - 1;
		int neg = *num == '-';
		num += neg;
		nnum -= neg;
		int val = *num++ - '0';
		--nnum;
		if (nnum > 2) {
			val = (val * 10) + (*num++ - '0');
		}
		++num;
		val = (val * 10) + (*num++ - '0');
		val *= 1 - (2 * neg);
		unsigned long long hash = FNV1A_OFFSET;
		for (int i = 0; i < nname; i++) {
			hash ^= (unsigned long long)name[i];
			hash *= FNV1A_PRIME;
		}
		struct station *stn = get_station(name, (unsigned)nname, hash);
		++stn->cnt;
		stn->max = stn->max > val ? stn->max : val;
		stn->min = stn->min < val ? stn->min : val;
		stn->sum += val;
	}
}

int
main(int argc, char *argv[])
{
	FILE *file = fopen(argc > 1 ? argv[1] : "measurements.txt", "rb");
	if (!file) goto end;
	fseek(file, 0, SEEK_END);
	size_t nfile = (size_t)ftell(file);
	fseek(file, 0, SEEK_SET);

	/* Make sure a batch can read at least one whole line. */
	size_t nbatch  = ROUND_UP_LINE(nfile / MAX_THREAD);
	size_t nthread = nfile / nbatch;
	size_t ntail   = ROUND_UP_LINE(nfile - nbatch * nthread);

	size_t nbuf = nbatch + MAX_LINE_LEN;
	/* TODO Replace with fixed size buffer and perform multiple
	 * reads per batch if requried.
	 */
	char *buf = malloc(nbuf);
	read_lines(buf, nbatch, 0, file);
	for (size_t i = 1; i < nthread; i++) {
		read_lines(buf, nbuf, 1, file);
	}
	if (ntail) {
		read_lines(buf, nbuf, 1, file);
	}

	printf("{");
	for (int i = 0; i < MAX_CAPACITY; i++) {
		if (!stations[i].cnt) continue;
		double avg = (double)stations[i].sum / stations[i].cnt;
		double min = (double)stations[i].min * 0.1;
		double max = (double)stations[i].max * 0.1;
		printf("%.*s=%.1f/%.1f/%.1f, ",
		        stations[i].nname, stations[i].name, min, avg, max);
	}
	/* TODO remove ", " from last entry */
	printf("}\n");

	fclose(file);
end:;
}

