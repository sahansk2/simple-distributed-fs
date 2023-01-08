package main

import (
	"amogus/fsys"
	"amogus/mp3util"
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"time"
)

func GetGzipFileSize(source io.Reader) (int64, error) {
	/*
		source ----> gzipConverter--------> compressWrite ----->(one-to-one BLOCKS)-----
																						|
																			io.Copy(  , |)

	*/
	compressRead, compressWrite := io.Pipe() // Anything written to compressWrite can be read by compressRead; therefore, N bytes written to `compressWrite` implies that N bytes are read from `compressRead`
	defer compressRead.Close()
	gzipConverter := gzip.NewWriter(compressWrite)
	go func() {
		defer compressWrite.Close()
		_, err := io.Copy(gzipConverter, source) // Writing to something that is ultimately an io pipe Write BLOCKS until a reader tries to read from it.
		if err != nil {
			fmt.Println("Couldn't read from source! Error: %v", err)
		}
		gzipConverter.Close()
		if err != nil {
			fmt.Println("Couldn't close gzip converter! Error: %v", err)
		}
	}()

	nbytes, err := io.Copy(io.Discard, compressRead)

	// HAVE to convert. This will block until the reader is called.

	/*
		goofy ahh diagram #2

		source
	*/
	if err != nil {
		fmt.Println("Could not measure compressed file size: ", err)
		return -1, err
	}

	if err = gzipConverter.Close(); err != nil {
		fmt.Println("Couldn't close gzipConverter! Error: ", err)
		return -1, err
	}
	mp3util.NodeLogger.Debugf("We expect the size of the gunzip to be: %v", nbytes)
	return nbytes, nil
}

func main() {
	mp3util.ConfigureLogger("bruh", "DEBUG", false)
	GZIPFILE := os.Args[2]
	ORIGINALFILE := os.Args[1]
	UNGZIPFILE := os.Args[3]
	gzipFile, err := os.OpenFile(GZIPFILE, os.O_RDWR|os.O_CREATE, os.ModePerm)
	if err != nil {
		fmt.Fprintf(os.Stderr, "couldn't open gzip target!")
		return
	}
	localFile, err := os.OpenFile(ORIGINALFILE, os.O_RDONLY, os.ModePerm)
	defer localFile.Close()
	if err != nil {
		fmt.Fprintf(os.Stderr, "couldn't open local file!")
		return
	}
	err = fsys.SendFileAsGzip(localFile, gzipFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "couldn't write gunzip!")
		return
	}
	_, err = gzipFile.Seek(0, 0)
	if err != nil {
		fmt.Fprintf(os.Stderr, "couldn't open gzip target for rereading!")
		return
	}

	ungzipFile, err := os.OpenFile(UNGZIPFILE, os.O_RDWR|os.O_CREATE, os.ModePerm)
	if err != nil {
		fmt.Fprintf(os.Stderr, "couldn't open ungzip target!")
		return
	}

	fi, err := os.Stat(GZIPFILE)
	if err != nil {
		fmt.Println("Couldn't stat file %v! Error: %v", fi, err)
		return
	}

	fmt.Println("Filesize of gzip file is: ", fi.Size())
	localFile.Seek(0, 0)
	size, err := GetGzipFileSize(localFile)
	fmt.Println("Size: %v", size)
	fmt.Println("Err: %v", err)

	nbytes, err := fsys.RecvFileFromGzip(
		&io.LimitedReader{
			R: gzipFile,
			N: fi.Size(),
		}, ungzipFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "couldn't ungunzip!")
		return
	}
	fmt.Fprintf(os.Stderr, "%v was read (raw), was gunzipped to %v, and was ungunzipped to %v. %v bytes were ungunzipped.",
		os.Args[1],
		os.Args[2],
		os.Args[3],
		nbytes)

	storage, err := fsys.NewSDFSStorage()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Couldn't initialize storage!")
		return
	}

	_, err = ungzipFile.Seek(0, 0)
	contentHash, err := storage.DumpBytesToTmpfile(&io.LimitedReader{R: ungzipFile, N: nbytes})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Couldn't dump bytes to tmpfile! %v\n", err)
		return
	}
	fmt.Fprintf(os.Stderr, "The content hash is: %v\n", contentHash)
	// Above here known works

	err = storage.RegisterTmpfileToSDFS(contentHash, time.Now(), "amogus")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error registering to tmpfile!  %v", err)
		return
	}
	//time.Sleep(1 * time.Second)
	//err = storage.RegisterTmpfileToSDFS(contentHash, time.Now(), "amogus")
	//if err != nil {
	//	fmt.Fprintf(os.Stderr, "Error registering to tmpfile!  %v", err)
	//	return
	//}
	//time.Sleep(1 * time.Second)
	//err = storage.RegisterTmpfileToSDFS(contentHash, time.Now(), "amogus")
	//if err != nil {
	//	fmt.Fprintf(os.Stderr, "Error registering to tmpfile!  %v", err)
	//	return
	//}
	//time.Sleep(1 * time.Second)
	//err = storage.RegisterTmpfileToSDFS(contentHash, time.Now(), "amogus")
	//if err != nil {
	//	fmt.Fprintf(os.Stderr, "Error registering to tmpfile!  %v", err)
	//	return
	//}
	vers, err := storage.AcquireFileHandles(5, "amogus", time.Now())
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error acquire file handles! %v", err)
		return
	}
	for _, v := range vers {
		fmt.Fprintf(os.Stderr, "Time: %v, Filename: %v\n", v.Version, v.SDFSFileName)
		v.Handle.Close()
	}

	a, err := storage.ListStoredSDFSFilesAllVersions()
	if err != nil {
		mp3util.NodeLogger.Debugf("Couldn't ls all versions!")
		return
	}
	fmt.Fprintf(os.Stderr, "The result is: ")
	fmt.Fprintf(os.Stderr, "%v", a)

	_, err = storage.RemoveSDFSFile("amogus", time.Now())
	if err != nil {
		mp3util.NodeLogger.Debugf("Bruh: %v", err)
	}
}
