LIBS :=-ldl -lpthread `mysql_config --libs`
CFLAG :=-Wall -g `mysql_config --include`

all: clean install

sqlite3.o:
	gcc -c sqlite3.c  -o sqlite3.o
test.o:
	gcc -c $(CFLAG) test.c -o test.o
install: sqlite3.o test.o
	gcc $(LIBS) $(CFLAG) sqlite3.o mysql2sqlite.o -o mysql2sqlite
clean:
	@if [ -f "mysql2sqlite" ] ; then \
		rm -rf mysql2sqlite ; \
	fi
	@if [ -f core ] ; then \
		rm core ; \
	fi
	@if [ -f mysql2sqlite.o ] ; then \
		rm mysql2sqlite.o ; \
	fi
