LIBS :=-ldl -lpthread `mysql_config --libs`
CFLAG :=-Wall -g `mysql_config --include`

all: clean install

sqlite3.o:
	gcc -c sqlite3.c  -o sqlite3.o
test.o:
	gcc -c $(CFLAG) test.c -o test.o
install: sqlite3.o test.o
	gcc $(LIBS) $(CFLAG) sqlite3.o test.o -o test
clean:
	@if [ -f "test" ] ; then \
		rm -rf test ; \
	fi
	@if [ -f core ] ; then \
		rm core ; \
	fi
	@if [ -f test.o ] ; then \
		rm test.o ; \
	fi
