FILE1 = application3.c

all: application3

application3: application3.c Makefile
	gcc -g -pthread -o application3 application3.c

clean:
	rm -rf application3 output*
