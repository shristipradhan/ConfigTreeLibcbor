configtreeget:
	cc cft.c streaming_get.c -lcbor -o cft

configtreeset:
	cc cft.c streaming_set.c -lcbor -o cft

configtreeerase:
	cc cft.c streaming_erase.c -lcbor -o cft

clean:
	rm cft