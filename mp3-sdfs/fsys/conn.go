package fsys

import (
	"amogus/mp3util"
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

type TCPChannelResponseCode string

const (
	OK             TCPChannelResponseCode = "OK"
	BAD_REQUEST    TCPChannelResponseCode = "BAD_REQUEST"
	MISC_ERROR     TCPChannelResponseCode = "INTERNAL_ERROR"
	FILE_NOT_FOUND TCPChannelResponseCode = "FILE_NOT_FOUND"
	NOTHING_TO_DO  TCPChannelResponseCode = "NOTHING_TO_DO"
)

type TCPChannelRequestType string

const (
	CLIENT_REQ_FILE_METADATA TCPChannelRequestType = "REQ_FILE_METADATA"
	CLIENT_REQ_FILE_DATA     TCPChannelRequestType = "REQ_FILE_DATA"
	CLIENT_REQ_KVERSIONS     TCPChannelRequestType = "REQ_K_VERSIONS"
	CLIENT_SEND_FILE_DATA    TCPChannelRequestType = "SEND_FILE_DATA"
	CLIENT_LIST_FILES        TCPChannelRequestType = "REQ_LIST_FILES"
	MASTER_FINALIZE_WRITE    TCPChannelRequestType = "FINALIZE_WRITE"
	MASTER_FINALIZE_DELETE   TCPChannelRequestType = "FINALIZE_DELETE"
	REPLICA_QUERY_FILES      TCPChannelRequestType = "QUERY_CONTAINED_FILES"
	REPLICA_SEND_FILE        TCPChannelRequestType = "REPLICA_SEND_FILE"
)

type TCPChannelRequest struct {
	RequestType       TCPChannelRequestType
	FileVersionSet    SDFSFileVersionSet
	SDFSFileVersion   int64
	FileSize          int64
	FileContentHash   string
	SDFSFileName      string
	KVersions         int
	UpperVersionBound int64
}

func (t *TCPChannelRequest) String() string {
	return fmt.Sprintf("TCPChannelRequest{ RequestType=%v, SDFSFileName=%v }", t.RequestType, t.SDFSFileName)
}

type TCPChannelResponse struct {
	ResponseCode            TCPChannelResponseCode
	ReturningSDFSFileSize   int64
	SDFSFileVersion         int64
	FileContentHash         string
	FileList                []SDFSFile
	RequestedFileVersionSet SDFSFileVersionSet
}

func (t *TCPChannelResponse) String() string {
	return fmt.Sprintf("TCPChannelResponse{ ResponseCode=%v,  ReturningSDFSFileSize=%v }", t.ResponseCode, t.ReturningSDFSFileSize)
}

/*
Returns a byte slice containing a buffer that can be json.Unmarshal'd. This shouldn't be used raw, use TCPChannelRequest/TCPChannelResponse's
Recv instead of this, because this can't keep track of types.

Blocking.
*/
func RecvStructJSON(conn io.Reader) ([]byte, error) {
	// Code to read the size of the JSON
	var jsonSize int64
	jsonSizeBytes := make([]byte, binary.Size(jsonSize))
	nbytes, err := io.ReadFull(conn, jsonSizeBytes)
	if err != nil {
		mp3util.NodeLogger.Error("Couldn't read size of JSON from TCP conn!")
		return nil, errors.New(fmt.Sprintf("RecvTCPChannelResponse only read %v bytes. Error: %v", nbytes, err))
	}

	err = binary.Read(bytes.NewBuffer(jsonSizeBytes), binary.LittleEndian, &jsonSize)
	if err != nil {
		mp3util.NodeLogger.Error("Couldn't decode JSON size from TCP conn!")
		return nil, errors.New(fmt.Sprintf("RecvTCPChannelResponse couldn't decode JSON size: %v", err))
	}

	// jsonSize is now valid. Prepare to read bytes
	jsonBytes := make([]byte, jsonSize)
	nbytes, err = io.ReadFull(conn, jsonBytes)
	if err != nil {
		mp3util.NodeLogger.Errorf("Couldn't read enough JSON bytes from TCP conn! Read %v bytes", nbytes)
		return nil, errors.New(fmt.Sprintf("RecvTCPChannelResponse couldn't read json bytes: %v", err))
	}

	return jsonBytes, nil
}

/*
Sends an arbitrary type that can be json.Marshal'd over the conn. This shouldn't be used raw, use TCPChannelRequest/TCPChannelResponse's
Send instead of this, because this can't keep track of types.

Blocking.
*/
func SendStructJSON(v interface{}, conn io.Writer) error {
	// Encode as JSON
	j, err := json.Marshal(v)
	if err != nil {
		mp3util.NodeLogger.Error(fmt.Sprintf("Couldn't marshall TCPChannelRequest: %v\n", err))
		return errors.New(fmt.Sprintf("TCPChannelRequest SendTo: %v", err))
	}
	// Trick: https://stackoverflow.com/a/26070729/6184823
	// Send the length
	var jsonSize = int64(len(j))
	err = binary.Write(conn, binary.LittleEndian, jsonSize) // send JSON length
	if err != nil {
		mp3util.NodeLogger.Error(fmt.Sprintf("Couldn't send JSON size: %v\n", err))
		return errors.New(fmt.Sprintf("TCPChannelRequest SendTo: %v", err))
	}

	// Send the JSON
	errorState := false
	nbytes, err := conn.Write(j)
	if int64(nbytes) != jsonSize {
		mp3util.NodeLogger.Warnf("Couldn't write all bytes of the JSON, expect an error")
		errorState = true
	}
	if err != nil {
		mp3util.NodeLogger.Error(fmt.Sprintf("Couldn't send JSON data: %v\n", err))
		return errors.New(fmt.Sprintf("TCPChannelRequest Send: %v", err))
	} else if errorState {
		mp3util.NodeLogger.Error("err was NIL but couldn't write all bytes of JSON. Debug this before proceeding.")
		return errors.New(fmt.Sprintf("Inconsistent error detection"))
	}
	return nil
}

/*
Blocking.
*/
func RecvTCPChannelRequest(conn io.Reader) (*TCPChannelRequest, error) {
	reqBytes, err := RecvStructJSON(conn)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("Can't receive a TCPChannelRequest! %v", err))
	}
	var req TCPChannelRequest
	err = json.Unmarshal(reqBytes, &req)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("Couldn't unmarshal a TCPChannelRequest! %v", err))
	}
	mp3util.NodeLogger.Debug("Deserialized TCPChannelRequest: ", req)
	return &req, nil
}

/*

Blocking.
*/
func RecvTCPChannelResponse(conn io.Reader) (*TCPChannelResponse, error) {
	respBytes, err := RecvStructJSON(conn)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("Can't receive a TCPChannelResponse! %v", err))
	}
	var resp TCPChannelResponse
	err = json.Unmarshal(respBytes, &resp)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("Couldn't unmarshal a TCPChannelResponse! %v", err))
	}
	mp3util.NodeLogger.Debug("Deserialized TCPChannelResponse: ", resp)
	if resp.ResponseCode != OK {
		return nil, errors.New(fmt.Sprintf("Received error code: %v", resp.ResponseCode))
	}

	return &resp, nil
}

func TrySendTCPChannelResponseError(conn io.Writer, code TCPChannelResponseCode) {
	err := (&TCPChannelResponse{
		ResponseCode: code,
	}).Send(conn)
	if err != nil {
		mp3util.NodeLogger.Error("Couldn't send back response via channel!")
	}
}

/*

Blocking.
*/
func (resp *TCPChannelResponse) Send(conn io.Writer) error {
	err := SendStructJSON(resp, conn)
	if err != nil {
		return errors.New(fmt.Sprintf("Can't receive a TCPChannelRequest! %v", err))
	}
	return nil
}

/*

Blocking.
*/
func (req *TCPChannelRequest) Send(conn io.Writer) error {
	err := SendStructJSON(req, conn)
	if err != nil {
		return errors.New(fmt.Sprintf("Can't receive a TCPChannelRequest! %v", err))
	}
	return nil
}

func GetGzipFileSize(source io.Reader) (int64, error) {
	/*
		source ----> gzipConverter--------> compressWrite ----->(one-to-one BLOCKS)----compressReader
																						|
																	nbytes=	io.Copy(| , |)
																					|-----------io.Discard
	*/

	compressRead, compressWrite := io.Pipe() // Anything written to compressWrite can be read by compressRead; therefore, N bytes written to `compressWrite` implies that N bytes are read from `compressRead`
	defer compressRead.Close()
	gzipConverter := gzip.NewWriter(compressWrite)
	go func() {
		defer compressWrite.Close()
		_, err := io.Copy(gzipConverter, source) // Writing to something that is ultimately an io pipe Write BLOCKS until a reader tries to read from it.
		if err != nil {
			mp3util.NodeLogger.Error("Couldn't read from source! Error: %v", err)
		}
		err = gzipConverter.Close()
		if err != nil {
			mp3util.NodeLogger.Error("Couldn't close gzip converter! Error: %v", err)
		}
	}()

	nbytes, err := io.Copy(io.Discard, compressRead)

	if err != nil {
		mp3util.NodeLogger.Error("Could not measure compressed file size: ", err)
		return nbytes, err
	}

	if err = gzipConverter.Close(); err != nil {
		mp3util.NodeLogger.Error("Couldn't close gzipConverter! Error: ", err)
		return nbytes, err
	}
	mp3util.NodeLogger.Debugf("We expect the size of the gunzip to be: %v", nbytes)
	return nbytes, nil
}

func SendFileAsGzip(source io.Reader, target io.Writer) error {
	gzipConverter := gzip.NewWriter(target)
	nbytes, err := io.Copy(gzipConverter, source)
	if err != nil {
		mp3util.NodeLogger.Error("Transferred %v bytes before encountering error! Error: ", err)
		err := gzipConverter.Close()
		if err != nil {
			mp3util.NodeLogger.Error("Couldn't close gzip writer! Error: ", err)
		}
		return err
	}
	err = gzipConverter.Close()
	if err != nil {
		mp3util.NodeLogger.Error("Couldn't close gzip writer! Error: ", err)
	}
	mp3util.NodeLogger.Infof("Transferred %v bytes (gzipped).", nbytes)
	return nil
}

/*
source is a io.Reader with file bytes that have been encoded as gunzip. This file writes the un-gunzipped data
to target.
*/
func RecvFileFromGzip(source *io.LimitedReader, target io.Writer) (int64, error) {
	fileTarget := target
	gzipDecoder, err := gzip.NewReader(source)
	if err != nil {
		mp3util.NodeLogger.Error("Couldn't initialize gzip decoder! Error: ", err)
		return 0, err
	}
	nbytes, err := io.Copy(fileTarget, gzipDecoder)
	mp3util.NodeLogger.Debugf("Copied %v bytes from conn to gzipDecoder", nbytes)
	if err != nil {
		mp3util.NodeLogger.Error("Error reading from channel providing gunzipped data! Error: ", err)
		return 0, err
	}

	err = gzipDecoder.Close()
	if err != nil {
		mp3util.NodeLogger.Error("Failed to close gzipDecoder: ", err)
		return 0, err
	}

	mp3util.NodeLogger.Infof("Received %v bytes and un-gunzipped.", nbytes)
	//err = fileTarget.Flush()
	//if err != nil {
	//	mp3util.NodeLogger.Error("Failed to flush file: ", err)
	//	return 0, err
	//}

	return nbytes, err
}
