#pragma once

#ifdef _MSC_VER
    #ifdef _DDBAPIDLL
        #define EXPORT_DECL _declspec(dllexport)
    #else
        #define EXPORT_DECL __declspec(dllimport)
    #endif
#else
    #define EXPORT_DECL 
#endif