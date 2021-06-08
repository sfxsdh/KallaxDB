CC=gcc  #If you use GCC, add -fno-strict-aliasing to the CFLAGS because the Google BTree does weird stuff
CFLAGS=-Wall -O0 -ggdb3 -ggdb -fPIC
CXX=g++
CXXFLAGS= ${CFLAGS} -std=c++11 -DUSE_RBTREE_WAL -I./include

LDLIBS=-L. -lm -lpthread -lstdc++ -lrocksdb

INDEXES_OBJ=indexes/btree.o 
OBJS=wal_rbtree.o slab.o ioengine.o pagecache.o stats.o xxhash.o random.o slabworker.o workload-common.o workload-ycsb.o workload-production.o utils.o  rocks_meta.o ${INDEXES_OBJ}
MAIN_OBJ=bench.o 

.PHONY: all clean

all: makefile.dep bench
	
makefile.dep: *.cc *.[Cch] indexes/*.[ch] indexes/*.cc
	##${CC} -c -O2
	for i in *.[Cc]; do ${CC} -MM "$${i}" ${CFLAGS}; done > $@
	for i in *.cc; do ${CXX} -MM "$${i}" -MT $${i%.cc}.o ${CXXFLAGS}; done >> $@
	for i in indexes/*.c; do ${CC} -MM "$${i}" -MT $${i%.c}.o ${CFLAGS}; done >> $@
	for i in indexes/*.cc; do ${CXX} -MM "$${i}" -MT $${i%.cc}.o ${CXXFLAGS}; done >> $@
	#find ./ -type f \( -iname \*.c -o -iname \*.cc \) | parallel clang -MM "{}" -MT "{.}".o > makefile.dep #If you find that the lines above take too long...

-include makefile.dep

all: libkallax.so bench

libkallax.so: $(OBJS)
	${CC} -shared -o $@ $^

bench: ${MAIN_OBJ} libkallax.so
	${CC} -o bench ${LDLIBS} -lkallax ${MAIN_OBJ}

clean:
	rm -f *.o indexes/*.o libkallax.so bench
