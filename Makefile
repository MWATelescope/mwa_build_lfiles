INCS=$(shell python -c "if len('${INCLUDE}')>0:print ' '.join(['-I ' + s for s in '${INCLUDE}'.split(':')])") -L${CFITSLIB} -I${CFITSINC} 
CFLAGS+=-Wall

all: build_lfiles read_mwac

read_mwac: read_mwac.o  
	$(CC) $(CFLAGS) read_mwac.o -o read_mwac ${INCS} -lcfitsio -lm

build_lfiles: build_lfiles.o mwac_utils.o antenna_mapping.o
	$(CC) $(CFLAGS) build_lfiles.o mwac_utils.o antenna_mapping.o -o build_lfiles ${INCS} -lcfitsio -lm

mwac_utils.o: mwac_utils.c
	$(CC) $(CFLAGS)  -c mwac_utils.c

build_lfiles.o: build_lfiles.c
	$(CC) $(CFLAGS) ${INCS} -c build_lfiles.c 

read_mwac.o: read_mwac.c
	$(CC) $(CFLAGS) ${INCS} -c read_mwac.c 

antenna_mapping.o: antenna_mapping.c
	$(CC) $(CFLAGS) -c antenna_mapping.c 

clean:
	rm -f *.o build_lfiles


