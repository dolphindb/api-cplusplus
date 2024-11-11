/*
 * Exceptions.h
 *
 *  Created on: Jul 22, 2012
 *      Author: dzhou
 */

#ifndef EXCEPTIONS_H_
#define EXCEPTIONS_H_

#include <exception>
#include <string>
#include "Types.h"
#include "Exports.h"

namespace dolphindb {

class IncompatibleTypeException: public std::exception{
public:
	IncompatibleTypeException(DATA_TYPE expected, DATA_TYPE actual)
		: expected_(expected), actual_(actual)
	{
		errMsg_.append("Incompatible type. Expected: " + getDataTypeName(expected_) + ", Actual: " + getDataTypeName(actual_));
	}

	virtual ~IncompatibleTypeException() throw(){}
	virtual const char* what() const throw() { return errMsg_.c_str();}
	DATA_TYPE expectedType(){return expected_;}
	DATA_TYPE actualType(){return actual_;}
private:
	DATA_TYPE expected_;
	DATA_TYPE actual_;
	std::string errMsg_;
};

class SyntaxException: public std::exception{
public:
	SyntaxException(const std::string& errMsg): errMsg_(errMsg){}
	virtual const char* what() const throw(){
		return errMsg_.c_str();
	}
	virtual ~SyntaxException() throw(){}

private:
	const std::string errMsg_;
};

class IllegalArgumentException : public std::exception{
public:
	IllegalArgumentException(const std::string& functionName, const std::string& errMsg): functionName_(functionName), errMsg_(errMsg){}
	virtual const char* what() const throw(){
		return errMsg_.c_str();
	}
	virtual ~IllegalArgumentException() throw(){}
	const std::string& getFunctionName() const { return functionName_;}

private:
	const std::string functionName_;
	const std::string errMsg_;
};

class RuntimeException: public std::exception{
public:
	RuntimeException(const std::string& errMsg):errMsg_(errMsg){}
	virtual const char* what() const throw(){
		return errMsg_.c_str();
	}
	virtual ~RuntimeException() throw(){}

private:
	const std::string errMsg_;
};

class OperatorRuntimeException: public std::exception{
public:
	OperatorRuntimeException(const std::string& optr,const std::string& errMsg): operator_(optr),errMsg_(errMsg){}
	virtual const char* what() const throw(){
		return errMsg_.c_str();
	}
	virtual ~OperatorRuntimeException() throw(){}
	const std::string& getOperatorName() const { return operator_;}

private:
	const std::string operator_;
	const std::string errMsg_;
};

class TableRuntimeException: public std::exception{
public:
	TableRuntimeException(const std::string& errMsg): errMsg_(errMsg){}
	virtual const char* what() const throw(){
		return errMsg_.c_str();
	}
	virtual ~TableRuntimeException() throw(){}

private:
	const std::string errMsg_;
};

class MemoryException: public std::exception{
public:
	MemoryException():errMsg_("Out of memory"){}
	virtual const char* what() const throw(){
		return errMsg_.c_str();
	}
	virtual ~MemoryException() throw(){}

private:
	const std::string errMsg_;
};

class IOException: public std::exception{
public:
	IOException(const std::string& errMsg): errMsg_(errMsg), errCode_(OTHERERR){}
	IOException(const std::string& errMsg, IO_ERR errCode): errMsg_(errMsg + ". " + getCodeDescription(errCode)), errCode_(errCode){}
	IOException(IO_ERR errCode): errMsg_(getCodeDescription(errCode)), errCode_(errCode){}
	virtual const char* what() const throw(){
		return errMsg_.c_str();
	}
	virtual ~IOException() throw(){}
	IO_ERR getErrorCode() const {return errCode_;}
private:
    std::string getCodeDescription(IO_ERR errCode) const
    {
        switch (errCode) {
        case OK: return "";
        case DISCONNECTED: return "Socket is disconnected/closed or file is closed.";
        case NODATA: return "In non-blocking socket mode, there is no data ready for retrieval yet.";
        case NOSPACE: return "Out of memory, no disk space, or no buffer for sending data in non-blocking socket mode.";
        case TOO_LARGE_DATA: return "String size exceeds 64K or code size exceeds 1 MB during serialization over network.";
        case INPROGRESS: return "In non-blocking socket mode, a program is in pending connection mode.";
        case INVALIDDATA: return "Invalid message format";
        case END_OF_STREAM: return "Reach the end of a file or a buffer.";
        case READONLY: return "File is readable but not writable.";
        case WRITEONLY: return "File is writable but not readable.";
        case NOTEXIST: return "A file doesn't exist or the socket destination is not reachable.";
        case OTHERERR: return "Unknown IO error.";
        default: return "";
        }
    }

private:
	const std::string errMsg_;
	const IO_ERR errCode_;
};

class DataCorruptionException: public std::exception {
public:
	DataCorruptionException(const std::string& errMsg) : errMsg_("<DataCorruption>" + errMsg){}
	virtual const char* what() const throw(){
		return errMsg_.c_str();
	}
	virtual ~DataCorruptionException() throw(){}

private:
	const std::string errMsg_;
};

class NotLeaderException: public std::exception {
public:
	//Electing a leader. Wait for a while to retry.
	NotLeaderException() : errMsg_("<NotLeader>"){}
	//Use the new leader specified in the input argument. format: <host>:<port>:<alias>, e.g. 192.168.1.10:8801:nodeA
	NotLeaderException(const std::string& newLeader) : errMsg_("<NotLeader>" + newLeader), newLeader_(newLeader){}
	const std::string& getNewLeader() const {return newLeader_;}
	virtual const char* what() const throw(){
		return errMsg_.c_str();
	}
	virtual ~NotLeaderException() throw(){}

private:
	const std::string errMsg_;
	const std::string newLeader_;
};

class MathException: public std::exception {
public:
	MathException(const std::string& errMsg) : errMsg_(errMsg){}
	virtual const char* what() const throw(){
		return errMsg_.c_str();
	}
	virtual ~MathException() throw(){}

private:
	const std::string errMsg_;
};

class TestingException: public std::exception{
public:
	TestingException(const std::string& caseName,const std::string& subCaseName): name_(caseName),subName_(subCaseName){
		if(subName_.empty())
			errMsg_="Testing case "+name_+" failed";
		else
			errMsg_="Testing case "+name_+"_"+subName_+" failed";
	}
	virtual const char* what() const throw(){
		return errMsg_.c_str();
	}
	const std::string& getCaseName() const {return name_;}
	const std::string& getSubCaseName() const {return subName_;}
	virtual ~TestingException() throw(){}

private:
	const std::string name_;
	const std::string subName_;
	std::string errMsg_;

};

class UserException: public std::exception{
public:
	UserException(const std::string exceptionType, const std::string& msg) : exceptionType_(exceptionType), msg_(msg){}
	virtual const char* what() const throw(){
		return msg_.c_str();
	}
	const std::string& getExceptionType() const { return exceptionType_;}
	const std::string& getMessage() const { return msg_;}
	virtual ~UserException() throw(){}
private:
	std::string exceptionType_;
	std::string msg_;
};

}
#endif /* EXCEPTIONS_H_ */
