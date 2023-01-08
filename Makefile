.PHONY: all sdfs querier mp2-import clean

all: sdfs querier mp2-import
clean:
	make -C ./mp2-import-cpp clean
	make -C ./mp3-sdfs clean
	make -C ./mp1 clean
mp2-import:
	make -C ./mp2-import-cpp
sdfs:
	make -C ./mp3-sdfs
querier:
	make -C ./mp1

