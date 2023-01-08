package fsys

import (
	"amogus/mp3util"
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"
)

type LocalSDFSStorage struct {
	RootDir    string
	tmpfileDir string
}

type SDFSFileHandle struct {
	SDFSFileName string
	Handle       *os.File
	Version      time.Time
	FileSize     int64
}

type SDFSFile struct {
	SDFSFileName string
	Version      int64
}

type SDFSFileVersions struct {
}

// One file, multiple versions. Implementing hashset of versions wibth a map[int64]bool. Why doesn't golang have a hashset? I'm in pain.
type SDFSFileVersionSet map[string]map[int64]bool

func (s *LocalSDFSStorage) String() string {
	return fmt.Sprintf("LocalSDFSStorage{ RootDir=%v, tmpfileDir=%v }", s.RootDir, s.tmpfileDir)
}

const (
	ROOTDIR        = "sdfs"
	TMPFILE_DIR    = "tmpfileDir"
	STOREDFILE_DIR = "storedfileDir"
	LOCALFILE_DIR  = "fetchedfiles" // he's an outlier
)

/*
Initialize the LocalSDFSStorage. This will completely destroy the sdfs directory if it finds that it exists.
Don't put your bitcoin in this folder! :)
*/
func NewSDFSStorage() (*LocalSDFSStorage, error) {
	// -pwd
	// 	 \----run_sdfs.sh
	//   \----sdfs (ROOTDIR)
	// 		    \----tmpfileDir (TMPFILE_DIR)
	// 	        \----storedfileDir (STOREDFILE_DIR)
	rootDir := filepath.Join(".", ROOTDIR)
	tmpfileDir := filepath.Join(rootDir, TMPFILE_DIR)
	// First, see if the whole directory exists. If so, we nuke it.
	err := os.Mkdir(rootDir, 0777)
	if os.IsExist(err) {
		mp3util.NodeLogger.Debugf("Directory %v already exists on initialization. Nuking now...", rootDir)
		err = os.RemoveAll(rootDir)
		if err != nil {
			mp3util.NodeLogger.Errorf("Couldn't remove the existing %v directory! Error: %v\n", rootDir, err)
			return nil, err
		}
	}
	// Create the directory storing the tempfiles.
	// TODO: Refactor so that we don't actually make the directory here
	err = os.MkdirAll(tmpfileDir, 0777)
	if err != nil {
		mp3util.NodeLogger.Errorf("Error creating directory %v: %v\n", tmpfileDir, err)
		return nil, err
	}
	err = os.MkdirAll(filepath.Join(rootDir, STOREDFILE_DIR), 0777)
	if err != nil {
		mp3util.NodeLogger.Errorf("Error creating directory %v: %v\n", tmpfileDir, err)
		return nil, err
	}
	var s LocalSDFSStorage
	s.RootDir = rootDir
	s.tmpfileDir = tmpfileDir

	return &s, nil
}

// Assumes that the client is actually sending the gunzipped version of the file through the connection.
func (s *LocalSDFSStorage) DumpBytesToTmpfile(byteSource *io.LimitedReader) (string, error) {
	mp3util.NodeLogger.Debugf("Attempting to write %v bytes to a tmpfile.\n", byteSource.N)
	// Blob target is a temporary filename that will only exist until we are able to calculate the SHA1.
	blobName := fmt.Sprintf("tmp-%v", time.Now().UnixNano())
	blobTarget := filepath.Join(s.tmpfileDir, blobName)
	blobFileWriter, err := os.OpenFile(blobTarget, os.O_CREATE|os.O_RDWR, os.ModePerm)
	blobWriter := bufio.NewWriter(blobFileWriter)
	if err != nil {
		mp3util.NodeLogger.Errorf("Couldn't open %v for writing! Error: %v\n", err)
		return "", err
	}
	hashWriter := sha256.New()
	hashTeeReader := io.TeeReader(byteSource, hashWriter)
	/*
		funny diagram because explaining this line-by line is not possible
			byteSource>-----hashTeeReader>--\
			                      |          \-->io.Copy()
						          V                   v            (actual file)
							 hashWriter          blobWriter------->blobTarget
							(contentHash)         (bufio)

		Best way to think of this is like a rope. You can "pull" data from a reader, and you "push" data to a writer.
		Tee readers are such that when you pull on it, it simultaneously pushes data to writers.

	*/
	nbytes, err := io.Copy(blobWriter, hashTeeReader)
	if err != nil {
		mp3util.NodeLogger.Errorf("Unable to read all bytes from underlying reader, only read %v bytes! Error: %v\n", nbytes, err)
		os.Remove(blobName)
		return "", err
	}
	// Flush bufio.
	err = blobWriter.Flush()
	if err != nil {
		mp3util.NodeLogger.Error("Couldn't flush bufio! Error: ", err)
	}

	hashName := hex.EncodeToString(hashWriter.Sum(nil))
	mp3util.NodeLogger.Debugf("Caclulated ContentHash of %v is %v. Renaming the file to %v...\n", blobName, hashName, hashName)
	err = os.Rename(blobTarget, filepath.Join(s.tmpfileDir, hashName))
	if err != nil {
		mp3util.NodeLogger.Errorf("Couldn't rename the file from %v to %v! Error: %v", blobTarget, hashName)
	}
	mp3util.NodeLogger.Debugf("Successfully written all data to tmpfile: %v\n", filepath.Join(s.tmpfileDir, hashName))
	return hashName, nil
}

/*
RegisterTmpfileToSDFS is used to identify a file blob that the client has uploaded, to convert it to a file "registered" with
SDFS.

Suppose contentHash=123456789ABCDEF, and this fn is called with version=111156363265365 (Unix Nano), sdfsFileName=amongus.
The directory structure will change from this:
-------sdfs/
		|----storedfileDir/
				|...
		|----tmpfileDir/
				|----123456789ABCDEF
				|...

To this:
-------sdfs/
		|----storedfileDir/
				|----amongus/
						|----111156363265365 (this is a Unix nano)
		|-----tmpfileDir/
				|...

Inspired by Git.
*/
func (s *LocalSDFSStorage) RegisterTmpfileToSDFS(contentHash string, version time.Time, sdfsFileName string) error {
	if _, err := os.Stat(filepath.Join(s.tmpfileDir, contentHash)); os.IsNotExist(err) {
		mp3util.NodeLogger.Errorf("Tmpfile with contentHash: %v not found.\n", contentHash)
		return errors.New("TmpfileNotPresent")
	}
	// This is the directory that will contain *all* the versions for this particular file (sdfsFileName)
	fileHome := filepath.Join(s.RootDir, STOREDFILE_DIR, sdfsFileName)
	err := os.MkdirAll(fileHome, 0777)
	if err != nil {
		if os.IsExist(err) {
			mp3util.NodeLogger.Debugf("Directory %v already exists. Continuing...\n", fileHome)
		} else {
			mp3util.NodeLogger.Errorf("Couldn't create directory %v! Error: %v\n", fileHome, err)
			return err
		}
	}
	tmpFilePath := filepath.Join(s.tmpfileDir, contentHash)
	newFilePath := filepath.Join(fileHome, fmt.Sprintf("%v", version.UnixNano()))
	// Handle this really weird edge case
	if _, err = os.Stat(newFilePath); !os.IsNotExist(err) && err != nil {
		mp3util.NodeLogger.Warnf("There already exists filename with this version %v. "+
			"Please verify no race condition has occurred. We will proceed and overwrite.", newFilePath)
		err = os.Remove(newFilePath)
		if err != nil {
			mp3util.NodeLogger.Error("Couldn't remove the file! Error: ", err)
			return err
		}
	}
	err = os.Rename(tmpFilePath, newFilePath) // This is golang being stupid. This is a mv.
	if err != nil {
		mp3util.NodeLogger.Errorf("Couldn't move %v to %v! Error: %v\n", tmpFilePath, newFilePath, err)
		return err
	}
	mp3util.NodeLogger.Debugf("Successfully registered file with contentHash=%v as %v in SDFS.", contentHash, newFilePath)
	return nil
}

/*
Return a slice of OPEN *os.File handles representing the `kLatest` latest versions of the file `sdfsFileName` in question.
The onus is on the caller to close the file handles.
*/
func (s *LocalSDFSStorage) AcquireFileHandles(kLatest int, sdfsFileName string, upperVersionBound time.Time) ([]SDFSFileHandle, error) {
	mp3util.NodeLogger.Debugf("Acquiring file handles for: %v", sdfsFileName)
	if kLatest <= 0 {
		mp3util.NodeLogger.Errorf("Cannot fetch %v latest versions, out of range.", kLatest)
		return []SDFSFileHandle{}, os.ErrInvalid
	}
	directoryWithFiles := filepath.Join(s.RootDir, STOREDFILE_DIR, sdfsFileName)
	mp3util.NodeLogger.Debugf("The filepath we will traverse is: %v", directoryWithFiles)
	_, err := os.Stat(directoryWithFiles)
	if os.IsNotExist(err) {
		mp3util.NodeLogger.Errorf("File directory for %v not found on this replica!", sdfsFileName)
		return nil, os.ErrNotExist
	}
	var handles []SDFSFileHandle
	err = filepath.WalkDir(directoryWithFiles, func(p string, d fs.DirEntry, err error) error {
		if d.Type() == os.ModeDir {
			mp3util.NodeLogger.Debugf("%v is a directory. Skipping...", p)
			return nil
		}
		mp3util.NodeLogger.Debugf("Attempting to open %v...", filepath.Join(directoryWithFiles, d.Name()))
		fd, err := os.Open(filepath.Join(directoryWithFiles, d.Name()))
		if err != nil {
			mp3util.NodeLogger.Errorf("Could not open file %v! Error: %v", p, err)
			return err
		}
		t, err := strconv.ParseInt(d.Name(), 10, 64)
		if err != nil {
			mp3util.NodeLogger.Errorf("Coudln't parse filename!")
			return err
		}
		fileTime := time.Unix(0, t)

		if fileTime.After(upperVersionBound) {
			return nil
		}
		h := SDFSFileHandle{
			SDFSFileName: sdfsFileName,
			Handle:       fd,
			Version:      time.Unix(0, t),
		}
		fi, err := os.Stat(filepath.Join(directoryWithFiles, d.Name()))
		if err != nil {
			mp3util.NodeLogger.Errorf("Couldn't stat file %v! Error: %v", fi, err)
			return err
		}
		h.FileSize = fi.Size()
		versionAsNumber, err := strconv.ParseInt(d.Name(), 10, 64) // The filename *is* the version so we parse it.
		if err != nil {
			mp3util.NodeLogger.Errorf("Filename %v could not be converted to int64!", d.Name())
			return err
		}
		h.Version = time.Unix(0, versionAsNumber) // Parse unix timestamp.
		handles = append(handles, h)
		return nil
	})
	sort.Slice(handles, func(i, j int) bool {
		return handles[i].Version.After(handles[j].Version)
	})
	numToReturn := kLatest
	if len(handles) < kLatest {
		numToReturn = len(handles)
	}
	mp3util.NodeLogger.Debugf("Versions to be returned are: %v", handles[:numToReturn])
	return handles[:numToReturn], nil
}

func (s *LocalSDFSStorage) ListDirectory() ([]SDFSFile, error) {
	var storedSDFSFiles []SDFSFile
	sdfsDir := filepath.Join(s.RootDir, STOREDFILE_DIR)
	_, err := os.Stat(sdfsDir)
	if os.IsNotExist(err) {
		mp3util.NodeLogger.Errorf("No sdfs dir found on this replica!")
		return nil, os.ErrNotExist
	}
	localSDFSFiles, err := os.ReadDir(sdfsDir)
	if err != nil {
		mp3util.NodeLogger.Debugf("Couldn't open the SDFS directory. Error: ", err)
		return nil, err
	}
	for _, f := range localSDFSFiles {
		handles, err := s.AcquireFileHandles(1, f.Name(), time.Now())
		defer CloseHandles(handles)
		if err != nil {
			mp3util.NodeLogger.Errorf("Couldn't get the latest version of file: %v!", f.Name())
			return nil, err
		}
		sdfsfile := SDFSFile{handles[0].SDFSFileName, handles[0].Version.UnixNano()}
		storedSDFSFiles = append(storedSDFSFiles, sdfsfile)
	}

	return storedSDFSFiles, nil
}

/*
Suppose we have a sequence of commands from master like so:
	Put Amogus(t=0); PutAmogus(t=1); PutAmogus(t=2); RemoveAmogus(t=3); PutAmogus(t=4)
Then, we should NOT delete all versions and instead only delete the versions that were prior to the removal command.
	E.g. the folder should only contain Amongus(t=4)

This returns a boolean. If the boolean is true, this means that the directory EXISTS after deletion. (Which means a write happened soon after, which means we have a partial delete).

I think we need a full replica delete, NOT just a quorum delete. Otherwise there are weird edge cases.
*/
func (s *LocalSDFSStorage) RemoveSDFSFile(sdfsFile string, timeOfDeletion time.Time) (bool, error) {
	// Check the maximum timestamp of the files that's in here.
	maxTimestamp := time.Unix(0, 0)
	if _, err := os.Stat(filepath.Join(s.RootDir, STOREDFILE_DIR, sdfsFile)); os.IsNotExist(err) {
		mp3util.NodeLogger.Warnf("SDFSFile %v not found on this replica.", sdfsFile)
		return false, err
	}
	err := filepath.WalkDir(filepath.Join(s.RootDir, STOREDFILE_DIR, sdfsFile), func(p string, d fs.DirEntry, err error) error {
		// First entry is always the directory!!!!!1111111111!!!
		if d.Type() == os.ModeDir {
			//mp3util.NodeLogger.Warnf("Not a file: %v", d.Name())
			return nil
		}
		tStamp, err := strconv.ParseInt(d.Name(), 10, 64)
		if err != nil {
			mp3util.NodeLogger.Debugf("Couldn't parse filename into a number! Offending file: %v", tStamp)
			return err
		}
		tStampTime := time.Unix(0, tStamp)
		if err != nil {
			mp3util.NodeLogger.Debugf("Couldn't parse filename %v to int64! Error: ", p, err)
			return err
		}
		if tStampTime.After(maxTimestamp) {
			maxTimestamp = tStampTime
		}
		return nil
	})
	if err != nil {
		mp3util.NodeLogger.Debugf("Encountered error when walking directory! Error: ", err)
		return false, err
	}

	mp3util.NodeLogger.Debug("The maximum timestamp is: ", maxTimestamp.UnixNano())
	if timeOfDeletion.After(maxTimestamp) {
		// Remove the entire directory
		err = os.RemoveAll(filepath.Join(s.RootDir, STOREDFILE_DIR, sdfsFile))
		if err != nil {
			mp3util.NodeLogger.Debugf("Couldn't remove the path: %v! Error: %v", filepath.Join(s.RootDir, STOREDFILE_DIR, sdfsFile), err)
			return false, err
		}
		return false, nil
	} else {
		preservedDirectory := false
		// Only remove the versions that are older than the removal timestamp.
		err = filepath.WalkDir(filepath.Join(s.RootDir, STOREDFILE_DIR, sdfsFile), func(p string, d fs.DirEntry, err error) error {
			tStamp, err := strconv.ParseInt(d.Name(), 10, 64)
			tStampTime := time.Unix(0, tStamp)
			if err != nil {
				mp3util.NodeLogger.Debugf("Couldn't parse filename %v to int64! Error: ", p, err)
				return err
			}
			if tStampTime.After(timeOfDeletion) {
				preservedDirectory = true
			} else {
				err = os.Remove(filepath.Join(s.RootDir, STOREDFILE_DIR, sdfsFile, d.Name()))
				if err != nil {
					mp3util.NodeLogger.Debugf("Couldn't remove stale version of file, %v! Error: %v", p, err)
					return err
				}
			}
			return nil
		})

		if !preservedDirectory {
			err = os.RemoveAll(filepath.Join(s.RootDir, STOREDFILE_DIR, sdfsFile))
			return true, nil
		} else {
			return false, nil
		}

	}
}

func (s *LocalSDFSStorage) ListStoredSDFSFilesAllVersions() (SDFSFileVersionSet, error) {
	files, err := s.ListDirectory()
	if err != nil {
		mp3util.NodeLogger.Errorf("Could not list directory! Error: ", err)
		return nil, err
	}

	returningSet := make(SDFSFileVersionSet)
	for _, fi := range files {
		fiVersionSet := make(map[int64]bool)
		err = filepath.WalkDir(filepath.Join(s.RootDir, STOREDFILE_DIR, fi.SDFSFileName), func(p string, d fs.DirEntry, err error) error {
			mp3util.NodeLogger.Debugf("%v ; %v ; %v", p, d, err)
			if d.Type() == os.ModeDir {
				mp3util.NodeLogger.Debugf("Not a file: %v", d.Name())
				return nil
			}
			timeAsUnix, err := strconv.ParseInt(d.Name(), 10, 64)
			if err != nil {
				mp3util.NodeLogger.Errorf("Couldn't parse %v relating to file %v as a unix timestamp!", d.Name(), fi.SDFSFileName)
				return err
			}
			fiVersionSet[timeAsUnix] = true
			return nil
		})

		if err != nil {
			mp3util.NodeLogger.Debugf("Failed to list files: err = %v", err)
		}
		returningSet[fi.SDFSFileName] = fiVersionSet
	}
	return returningSet, nil
}

/*
fromSet: what replica has
toSet: what WE have

This ONLY reports deletions. You can calc additions by calling with reversed arguments.
	diff({x, y, z}, {y, z, b}) ----> {x}

*/
func DiffForDeletes(fromSet SDFSFileVersionSet, toSet SDFSFileVersionSet) SDFSFileVersionSet {
	deletions := make(SDFSFileVersionSet)
	for fi := range fromSet {
		_, presentInToSet := toSet[fi]
		// Case 1: Not present in toSet.
		if !presentInToSet {
			deletions[fi] = fromSet[fi] // All the fromVersions are missing; add them all to the diff.
			continue
		}

		// Case 2: (behold an unthinkable) Present in toSet but some versions are missing.
		fileDels := make(map[int64]bool)
		toSetHash := make(map[int64]bool)
		fromSetHash := make(map[int64]bool)
		for v, _ := range toSet[fi] {
			toSetHash[v] = true
		}
		for v, _ := range fromSet[fi] {
			fromSetHash[v] = true
		}

		// Calculate deletions
		for ver := range fromSetHash {
			_, found := toSetHash[ver]
			if !found {
				fileDels[ver] = true
			}
		}
		// Don't set the map if we have no file deletions to do!!!
		if len(fileDels) > 0 {
			deletions[fi] = fileDels
		}
	}
	return deletions
}

/*
Returns a set which, when merged with alreadyHaveSet, will contain up to K latest versions for each file. The second
argument would be the versions that, would be discarded by alreadyHaveSet.

For example, for a specific file X, if alreadyHaveSet[x] = {7, 5, 3, 2, 1}, and offeredSet[x] = {9, 5, 4, 3, 2}, k=5, then
the returned value will be ({9, 4},) because the K=5 latest versions would be
	{ 9 (theirs), 7 (ours), 5 (both have it, so skip), 4 (theirs), 3 (ours) }.
Anything that's "theirs" is what we have to fetch.

This is leetcode now.

This function exists so that we don't unknowingly fetch really stale versions and waste bandwidth.
But, then again, we ARE talking over JSON almost always...
But then again, when file sizes are ~1.25GB like the wikipedia corpus, JSON is but a drop in the ocean.

*/
func MergedKLatestVersions(alreadyHaveSet map[int64]bool, offeredSet map[int64]bool, k int) (fetch map[int64]bool, discard map[int64]bool) {
	type VersionFromWho struct {
		us_or_them_or_both string
		version            int64
	}

	var allMerged []VersionFromWho
	for ver := range alreadyHaveSet {
		_, existsInOffered := offeredSet[ver]
		if existsInOffered {
			continue
			// Commenting this out, because we don't *actually* care.
			//allMerged = append(allMerged, VersionFromWho{
			//	us_or_them_or_both: "both",
			//	version:            ver,
			//})
		} else {
			allMerged = append(allMerged, VersionFromWho{
				us_or_them_or_both: "us",
				version:            ver,
			})
		}
	}

	for ver := range offeredSet {
		_, existsInAlreadyHave := alreadyHaveSet[ver]
		if existsInAlreadyHave {
			continue // Already considered "both" in the prior loop.
		} else {
			allMerged = append(allMerged, VersionFromWho{
				us_or_them_or_both: "them",
				version:            ver,
			})
		}
	}
	sort.Slice(allMerged, func(i, j int) bool {
		timeI := time.Unix(0, allMerged[i].version)
		timeJ := time.Unix(0, allMerged[j].version)
		return timeI.After(timeJ)
	})
	mp3util.NodeLogger.Debugf("After sorting, allMerged contains: %v", allMerged)

	fetch = make(map[int64]bool)
	discard = make(map[int64]bool)
	for i := 0; i < len(allMerged); i++ {
		if i < k { // We're still
			if allMerged[i].us_or_them_or_both == "them" {
				fetch[allMerged[i].version] = true
			}
		} else { // Anything in "us" should be added to `discard`.
			if allMerged[i].us_or_them_or_both == "us" {
				discard[allMerged[i].version] = true
			}
		}
	}
	return fetch, discard
}

/*
Deferrable statement to close all file handle
*/
func CloseHandles(handles []SDFSFileHandle) {
	for _, handle := range handles {
		err := handle.Handle.Close()
		if err != nil {
			mp3util.NodeLogger.Error("Failed to close file: ", handle.SDFSFileName)
			continue
		}
	}
}
