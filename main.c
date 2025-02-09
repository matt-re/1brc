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
	int nname;
	char name[100];
	char pad[8];
};

struct station stations[MAX_CAPACITY];

static struct station *
get_station(char *name, int nname, unsigned long long hash)
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
		if (stations[i].nname == nname && memcmp(stations[i].name, name, (unsigned)nname) == 0) {
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
	char *beg;
	if (rest) {
		/* When in batch [1,N] need to start from a partial line from
		 * the previous batch.
		 */
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
		unsigned long long hash = FNV1A_OFFSET;
		while (*cur != ';') {
			hash ^= (unsigned long long)*cur;
			hash *= FNV1A_PRIME;
			++cur;
		}
		int nname = (int)(cur - name);
		++cur;
		int neg = *cur == '-';
		cur += neg;
		int num = *cur++ - '0';
		if (*cur != '.') {
			num = (num * 10) + (*cur++ - '0');
		}
		++cur;
		num = (num * 10) + (*cur++ - '0');
		num *= 1 - (2 * neg);
		++cur;
		struct station *stn = get_station(name, nname, hash);
		++stn->cnt;
		stn->max = stn->max > num ? stn->max : num;
		stn->min = stn->min < num ? stn->min : num;
		stn->sum += num;
	}
}

static int
compare(const void *a, const void *b)
{
	struct station *x = (struct station *)a;
	struct station *y = (struct station *)b;
	if (!x->cnt && !y->cnt)
		return 0;
	if (!x->cnt)
		return 1;
	if (!y->cnt)
		return 1;
	char *s1 = x->name;
	char *s2 = y->name;
	int n1 = x->nname;
	int n2 = y->nname;
	int n = n1 < n2 ? n1 : n2;
	int cmp = memcmp(s1, s2, (unsigned)n);
	if (cmp == 0)
		/* Equal so far, so shorter string is lexicographically
		 * smaller. Sign is what matters and not the number.
		 */
		return n1 - n2;
	return cmp;
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

	qsort(stations, MAX_CAPACITY, sizeof stations[0], compare);
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

