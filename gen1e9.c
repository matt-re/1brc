#include <fcntl.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#define MAX_STATIONS	10000
#define MAX_NAME_LEN	100
#define OUT_SIZE	(1 << 20)

struct name
{
	uint8_t len;
	uint8_t buf[MAX_NAME_LEN];
};

static struct name g_names[MAX_STATIONS];
static int32_t g_nnames;
static char g_out[OUT_SIZE + 256];

/* xoshiro256** PRNG */

static uint64_t g_s[4];

static uint64_t
rotl(uint64_t x, int k)
{
	return (x << k) | (x >> (64 - k));
}

static uint64_t
xoshiro256ss(void)
{
	uint64_t result = rotl(g_s[1] * 5, 7) * 9;
	uint64_t t = g_s[1] << 17;
	g_s[2] ^= g_s[0];
	g_s[3] ^= g_s[1];
	g_s[1] ^= g_s[2];
	g_s[0] ^= g_s[3];
	g_s[2] ^= t;
	g_s[3] = rotl(g_s[3], 45);
	return result;
}

static uint64_t
splitmix64(uint64_t *state)
{
	uint64_t z = (*state += UINT64_C(0x9E3779B97F4A7C15));
	z = (z ^ (z >> 30)) * UINT64_C(0xBF58476D1CE4E5B9);
	z = (z ^ (z >> 27)) * UINT64_C(0x94D049BB133111EB);
	return z ^ (z >> 31);
}

static void
seed(uint64_t val)
{
	uint64_t state = val;
	g_s[0] = splitmix64(&state);
	g_s[1] = splitmix64(&state);
	g_s[2] = splitmix64(&state);
	g_s[3] = splitmix64(&state);
}

/* station name loading */

static bool
nameeq(struct name *a, uint8_t *buf, uint8_t len)
{
	return a->len == len && memcmp(a->buf, buf, len) == 0;
}

static bool
loadnames(const char *file)
{
	FILE *fp = fopen(file, "rb");
	if (!fp) {
		perror(file);
		return false;
	}
	char line[256];
	while (fgets(line, (int)sizeof line, fp)) {
		char *semi = strchr(line, ';');
		if (!semi)
			continue;
		uint8_t len = (uint8_t)(semi - line);
		if (len == 0 || len > MAX_NAME_LEN)
			continue;
		/* linear dedup */
		bool found = false;
		for (int32_t i = 0; i < g_nnames; i++) {
			if (nameeq(&g_names[i], (uint8_t *)line, len)) {
				found = true;
				break;
			}
		}
		if (!found) {
			if (g_nnames >= MAX_STATIONS) {
				fprintf(stderr, "too many station names\n");
				fclose(fp);
				return false;
			}
			g_names[g_nnames].len = len;
			memcpy(g_names[g_nnames].buf, line, len);
			g_nnames++;
		}
	}
	fclose(fp);
	if (g_nnames == 0) {
		fprintf(stderr, "no station names found in %s\n", file);
		return false;
	}
	return true;
}

/* temperature formatting: val in [-999..999] as tenths -> e.g. -12.3 */

static int
fmttemp(char *buf, int32_t val)
{
	char *p = buf;
	if (val < 0) {
		*p++ = '-';
		val = -val;
	}
	int32_t whole = val / 10;
	if (whole >= 10)
		*p++ = (char)('0' + whole / 10);
	*p++ = (char)('0' + whole % 10);
	*p++ = '.';
	*p++ = (char)('0' + val % 10);
	return (int)(p - buf);
}

static bool
generate(int64_t nrows)
{
	char *p = g_out;
	for (int64_t i = 0; i < nrows; i++) {
		uint64_t r = xoshiro256ss();
		int32_t idx = (int32_t)(r % (uint64_t)g_nnames);
		int32_t temp = (int32_t)((xoshiro256ss() % 1999) - 999);

		memcpy(p, g_names[idx].buf, g_names[idx].len);
		p += g_names[idx].len;
		*p++ = ';';
		p += fmttemp(p, temp);
		*p++ = '\n';

		if (p - g_out >= OUT_SIZE) {
			ssize_t n = write(STDOUT_FILENO, g_out, (size_t)(p - g_out));
			if (n < 0) {
				perror("write");
				return false;
			}
			p = g_out;
		}
	}
	/* flush remainder */
	if (p > g_out) {
		ssize_t n = write(STDOUT_FILENO, g_out, (size_t)(p - g_out));
		if (n < 0) {
			perror("write");
			return false;
		}
	}
	return true;
}

int
main(int argc, char *argv[])
{
	int64_t nrows = 1000000000LL;
	const char *source = "measurements.txt";

	if (argc > 1)
		nrows = atoll(argv[1]);
	if (argc > 2)
		source = argv[2];
	if (nrows <= 0) {
		fprintf(stderr, "usage: %s [nrows] [source_file]\n", argv[0]);
		return 1;
	}

	seed((uint64_t)time(NULL));

	if (!loadnames(source))
		return 1;

	fprintf(stderr, "generating %lld rows from %d stations\n", (long long)nrows, g_nnames);

	if (!generate(nrows))
		return 1;

	return 0;
}
