
CFLAGS=-g -O -Wall -D_FILE_OFFSET_BITS=64
CFITSIO_INCS=$(shell pkg-config --silence-errors --cflags cfitsio)
CFITSIO_LIBS=$(shell pkg-config --silence-errors --libs cfitsio)

# if for some reason you need to manually specify CFITSIO stuff, then it should be something like:
#CFITSIO_INCS=-I/some/path/to/include
#CFITSIO_LIBS=-L/some/path/to/lib -lciftsio

# by default installs to /usr/local/bin. Override with make prefix=<basedir> install
# where basedir contains a directory called bin. e.g. make prefix=$HOME

# set default install directory
prefix = /usr/local
INSTALL=install

TARGETS=build_lfiles read_mwac uvcompress

all: $(TARGETS)

install: all
	mkdir -p $(prefix)/bin
	$(INSTALL) $(TARGETS) $(prefix)/bin

read_mwac: read_mwac.c
	$(CC) $(CFLAGS) $(CFITSIO_INCS) $^ -o $@ $(CFITSIO_LIBS) -lm

build_lfiles: build_lfiles.c mwac_utils.c antenna_mapping.c
	$(CC) $(CFLAGS) $(CFITSIO_INCS) -fopenmp $^ -o $@  $(CFITSIO_LIBS) -lm -lpthread

uvcompress: compress.cpp uvcompress.cpp
	$(CXX) $(CFLAGS) $(CFITSIO_INCS) $^ -o $@ $(CFITSIO_LIBS) -lm

clean:
	rm -f *.o $(TARGETS)

