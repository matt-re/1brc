#include <limits.h>
#include <pthread.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#define READ_SIZE	(1 << 23)
#ifndef MAX_THREAD
#define MAX_THREAD	1
#endif /* MAX_THREAD */
#define MAX_LINE_LEN	107
#define MAX_CAPACITY	(1 << 15)
#define FNV1A_OFFSET	UINT64_C(14695981039346656037)
#define FNV1A_PRIME	UINT64_C(1099511628211)

struct station
{
	uint64_t hash;
	int32_t max;
	int32_t min;
	int32_t sum;
	int32_t cnt;
	int32_t nname;
	uint8_t name[100];
};

struct data
{
	struct station *stn;
	uint8_t *buf;
	ptrdiff_t cap;
	ptrdiff_t len;
	ptrdiff_t off;
	int fd;
};

static struct station g_stations[MAX_THREAD][MAX_CAPACITY];
static uint8_t g_readbuffers[MAX_THREAD][READ_SIZE];
static pthread_t g_threads[MAX_THREAD];
static struct data g_data[MAX_THREAD];

static struct station *
find(uint8_t *name, int32_t nname, uint64_t hash, struct station *stn)
{
	uint64_t i = hash & (MAX_CAPACITY - 1);
	for (;;) {
		if (!stn[i].cnt) {
			stn[i].cnt = 0;
			stn[i].max = INT_MIN;
			stn[i].min = INT_MAX;
			stn[i].sum = 0;
			memcpy(stn[i].name, name, (unsigned)nname);
			stn[i].nname = nname;
			stn[i].hash = hash;
			break;
		} else if (stn[i].nname == nname && stn[i].hash == hash && memcmp(stn[i].name, name, (unsigned)nname) == 0) {
			break;
		} else {
			i = (i + 1) & (MAX_CAPACITY - 1);
		}
	}
	return &stn[i];
}

static void
processlines(uint8_t *beg, uint8_t *end, struct station *stations)
{
	uint8_t *cur = beg;
	while (cur < end) {
		uint8_t *name = cur;
		uint64_t hash = FNV1A_OFFSET;
		while (*cur != ';') {
			hash ^= *cur;
			hash *= FNV1A_PRIME;
			++cur;
		}
		int32_t nname = (int32_t)(cur - name);
		++cur;
		int32_t neg = *cur == '-';
		cur += neg;
		int32_t num = (*cur++ - '0') * 10;
		int32_t deca = *cur != '.';
		num *= 1 + 9 * deca;
		num += (*cur - '0') * 10 * deca;
		cur += deca + 1;
		num += *cur++ - '0';
		num *= 1 - (2 * neg);
		++cur;
		struct station *stn = find(name, nname, hash, stations);
		++stn->cnt;
		stn->max = stn->max > num ? stn->max : num;
		stn->min = stn->min < num ? stn->min : num;
		stn->sum += num;
	}
}

static ptrdiff_t
processbuffer(uint8_t *beg, uint8_t *end, bool lookback, struct station *stations)
{
	/* shift the beginning and end of the buffer to only read whole lines */
	if (lookback) {
		beg += MAX_LINE_LEN;
		while (*--beg !='\n');
		++beg;
	}
	uint8_t *oldend = end;
	while (*--end != '\n');
	++end;
	processlines(beg, end, stations);
	return oldend - end;
}

static void
processfile(int fd, uint8_t *buf, ptrdiff_t cap, ptrdiff_t len, ptrdiff_t offset, struct station *stations)
{
	/* Each batch after first needs to contain the characters from the
	 * previous batch to handle a line being split across batches. The
	 * current batch will only read up to the last \n character, which
	 * means some characters in the current batch will be ignored. The next
	 * batch will read characters from the end of the previous batch, at
	 * most one extea whole line.
	 */
	bool lookback = offset > 0;
	if (lookback) {
		offset -= MAX_LINE_LEN;
		len += MAX_LINE_LEN;
	}
	ptrdiff_t left = 0;
	do {
		ptrdiff_t avail = cap - left;
		ptrdiff_t amount = avail < len ? avail : len;
		ptrdiff_t nread = pread(fd, buf + left, (size_t)amount, offset);
		len -= nread;
		offset += nread;
		uint8_t *end = buf + nread + left;
		left = processbuffer(buf, end, lookback, stations);
		if (left)
			memmove(buf, end - left, left);
		lookback = false;
	} while (len);
}

static int32_t
compare(const void *a, const void *b)
{
	struct station *x = (struct station *)a;
	struct station *y = (struct station *)b;
	int res;
	if (x->cnt == 0 && y->cnt == 0)
		res = 0;
	else if (x->cnt == 0)
		res = 1;
	else if (y->cnt == 0)
		res = -1;
	else {
		int32_t n1 = x->nname;
		int32_t n2 = y->nname;
		int32_t minn = n1 < n2 ? n1 : n2;
		res = memcmp(x->name, y->name, (unsigned)minn);
		if (res == 0)
			res = n1 - n2;
	}
	return res;
}

static ptrdiff_t
getsize(char *file)
{
	struct stat st;
	stat(file, &st);
	ptrdiff_t size = st.st_size;
	return size > 0 ? size : 0;
}

static struct station *
merge(struct station *first, ptrdiff_t size, ptrdiff_t count)
{
	struct station *res = first;
	struct station *src = first + size;
	ptrdiff_t n = (count - 1) * size;
	for (ptrdiff_t i = 0; i < n; i++, src++) {
		if (src->cnt) {
			struct station *dst = find(src->name, src->nname, src->hash, res);
			dst->cnt += src->cnt;
			dst->sum += src->sum;
			dst->max = dst->max > src->max ? dst->max : src->max;
			dst->min = dst->min < src->min ? dst->min : src->min;
		}
	}
	return res;
}

static void *
threadstart(void *arg)
{
	struct data *d = arg;
	processfile(d->fd, d->buf, d->cap, d->len, d->off, d->stn);
	return arg;
}

int
main(int argc, char *argv[])
{
	char *file = argc > 1 ? argv[1] : "measurements.txt";
	ptrdiff_t nfile = getsize(file);
	if (!nfile)
		return 1;
	int fd = open(file, O_RDONLY);
	if (fd < 0)
		return 1;
	ptrdiff_t nbatch = nfile / MAX_THREAD;
	nbatch = nbatch < MAX_LINE_LEN ? MAX_LINE_LEN : nbatch;
	ptrdiff_t nthread = nfile / nbatch;
	for (ptrdiff_t i = 0, offset = 0; i < nthread; i++, offset += nbatch) {
		g_data[i] = (struct data){
			.fd  = fd,
			.buf = g_readbuffers[i],
			.cap = sizeof g_readbuffers[i],
			.len = nbatch,
			.off = offset,
			.stn = g_stations[i]
		};
	}
	ptrdiff_t ntail = nfile - nbatch * nthread;
	g_data[nthread-1].len += ntail;
	for (ptrdiff_t i = 0; i < nthread; i++)
		pthread_create(&g_threads[i], NULL, threadstart, &g_data[i]);
	for (ptrdiff_t i = 0; i < nthread; i++)
		pthread_join(g_threads[i], NULL);
	struct station *result = merge(g_stations[0], MAX_CAPACITY, nthread);
	qsort(result, MAX_CAPACITY, sizeof *result, compare);
	float avg = result[0].sum * 0.1f / result[0].cnt;
	float min = result[0].min * 0.1f;
	float max = result[0].max * 0.1f;
	printf("{%.*s=%.1f/%.1f/%.1f", result[0].nname, result[0].name, min, avg, max);
	for (size_t i = 1; (result[i].cnt > 0) && (i < MAX_CAPACITY); i++) {
		avg = result[i].sum * 0.1f / result[i].cnt;
		min = result[i].min * 0.1f;
		max = result[i].max * 0.1f;
		printf(", %.*s=%.1f/%.1f/%.1f", result[i].nname, result[i].name, min, avg, max);
	}
	printf("}\n");
	close(fd);
}
