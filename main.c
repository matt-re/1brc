#include <limits.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define READ_SIZE		4096
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

static struct station g_stations[MAX_CAPACITY];
static char g_readbuffer[READ_SIZE];

static struct station g_stations_mt[MAX_THREAD][MAX_CAPACITY];
static char g_readbuffers_mt[MAX_THREAD][READ_SIZE];

static struct station *
get_station(char *name, int nname, unsigned long long hash, struct station *stations)
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

static size_t
read_lines(char *buf, size_t len, int seek, struct station *stations)
{
	char *beg;
	if (seek) {
		beg = buf + MAX_LINE_LEN;
		while (*--beg !='\n');
		++beg;
	} else {
		beg = buf;
	}
	char *end = buf + len;
	while (*--end != '\n');
	++end;
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
		struct station *stn = get_station(name, nname, hash, stations);
		++stn->cnt;
		stn->max = stn->max > num ? stn->max : num;
		stn->min = stn->min < num ? stn->min : num;
		stn->sum += num;
	}
	return (size_t)((buf + len) - end);
}

static void
process_batch(char *buf, size_t nbuf, size_t len, int seek, FILE *stream, struct station *stations)
{
	size_t left = 0;
	for (;;) {
		if (!len) {
			break;
		}
		size_t cap = nbuf - left;
		size_t amount = cap < len ? cap : len;
		size_t nread = fread(buf + left, 1, amount, stream);
		if (!nread) {
			break;
		}
		len -= nread;
		size_t n = nread + left;
		left = read_lines(buf, n, seek, stations);
		if (left) {
			memmove(buf, buf + n - left, left);
		}
		seek = 0;
	}
}

static void
process_file(char *filename, char *buf, size_t nbuf, size_t len, size_t offset, struct station *stations)
{
	FILE *fp = fopen(filename, "rb");
	if (!fp)
		return;
	/* Each batch after first needs to contain the characters from the
	 * previous batch to handle a line being split across batches. The
	 * current batch will only read up to the last \n character, which
	 * means some characters in the current batch will be ignored. The next
	 * batch will read characters from the end of the previous batch, at
	 * most one extea whole line.
	 */
	if (offset) {
		fseek(fp, (ssize_t)(offset - MAX_LINE_LEN), SEEK_SET);
		len += MAX_LINE_LEN;
	}
	process_batch(buf, nbuf, len, offset > 0, fp, stations);
	fclose(fp);
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

static size_t
get_file_size(char *file)
{
	FILE *fp = fopen(file, "rb");
	if (!fp)
		return (size_t)-1;
	fseek(fp, 0, SEEK_END);
	size_t size = (size_t)ftell(fp);
	fseek(fp, 0, SEEK_SET);
	fclose(fp);
	return size;
}

static void
merge(struct station *dst, struct station *src)
{
	for (int i = 0; i < MAX_CAPACITY; i++) {
		if (!src[i].cnt) continue;
		unsigned long long hash = FNV1A_OFFSET;
		for (int s = 0; s < src[i].nname; s++) {
			hash ^= (unsigned long long)src[i].name[s];
			hash *= FNV1A_PRIME;
		}
		struct station *d = get_station(src[i].name, src[i].nname, hash, dst);
		d->cnt += src[i].cnt;
		d->max = d->max > src[i].max ? d->max : src[i].max;
		d->min = d->min < src[i].min ? d->min : src[i].min;
		d->sum += src[i].sum;
	}
}

int
main(int argc, char *argv[])
{
	char *filename = argc > 1 ? argv[1] : "measurements.txt";
	size_t nfile = get_file_size(filename);
	if (nfile == (size_t)-1) {
		goto end;
	}

	/* Make sure a batch can read at least one whole line. */
	size_t nbatch  = ROUND_UP_LINE(nfile / MAX_THREAD);
	/* How many extra threads [0,MAX_THREAD-1] */
	size_t nthread = nfile / nbatch;
	size_t offset = 0;
	for (size_t i = 0; i < nthread; i++) {
		process_file(filename, g_readbuffers_mt[i], sizeof g_readbuffers_mt[i], nbatch, offset, g_stations_mt[i]);
		offset += nbatch;
	}
	size_t ntail = nfile - nbatch * nthread;
	if (ntail) {
		process_file(filename, g_readbuffer, sizeof g_readbuffer, ntail, offset, g_stations);
	}

	for (size_t i = 0; i < nthread; i++) {
		merge(g_stations, g_stations_mt[i]);
	}
	qsort(g_stations, MAX_CAPACITY, sizeof g_stations[0], compare);
	printf("{");
	for (int i = 0; i < MAX_CAPACITY; i++) {
		if (!g_stations[i].cnt) continue;
		double avg = (double)g_stations[i].sum / g_stations[i].cnt;
		double min = (double)g_stations[i].min * 0.1;
		double max = (double)g_stations[i].max * 0.1;
		printf("%.*s=%.1f/%.1f/%.1f, ", g_stations[i].nname, g_stations[i].name, min, avg, max);
	}
	/* TODO remove ", " from last entry */
	printf("}\n");
end:;
}

