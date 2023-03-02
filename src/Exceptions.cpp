/*
 * Exceptions.cpp
 *
 *  Created on: Feb 11, 2016
 *      Author: dzhou
 */
#include "Exceptions.h"
#include "Util.h"

namespace dolphindb {

IncompatibleTypeException::IncompatibleTypeException(DATA_TYPE expected, DATA_TYPE actual) : expected_(expected), actual_(actual){
	errMsg_.append("Incompatible type. Expected: " + Util::getDataTypeString(expected_) + ", Actual: " + Util::getDataTypeString(actual_));
}

string IOException::getCodeDescription(IO_ERR errCode) const {
	switch(errCode){
	case OK :
		return "";
	case DISCONNECTED :
		return "Socket is disconnected/closed or file is closed.";
	case NODATA :
		return "In non-blocking socket mode, there is no data ready for retrieval yet.";
	case NOSPACE :
		return "Out of memory, no disk space, or no buffer for sending data in non-blocking socket mode.";
	case TOO_LARGE_DATA :
		return "String size exceeds 64K or code size exceeds 1 MB during serialization over network.";
	case INPROGRESS :
		return "In non-blocking socket mode, a program is in pending connection mode.";
	case INVALIDDATA :
		return "Invalid message format";
	case END_OF_STREAM :
		return "Reach the end of a file or a buffer.";
	case READONLY :
		return "File is readable but not writable.";
	case WRITEONLY :
		return "File is writable but not readable.";
	case NOTEXIST :
		return "A file doesn't exist or the socket destination is not reachable.";
	case OTHERERR :
		return "Unknown IO error.";
	default:
		return "";
	}
}

};
