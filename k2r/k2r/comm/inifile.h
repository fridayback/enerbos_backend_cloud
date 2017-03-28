/**
 * @file
 * @brief initialization file read and write API
 *	-size of the ini file must less than 16K
 *	-after '=' in key value pair, can not support empty char. this would not like WIN32 API
 *	-support comment using ';' prefix
 *	-can not support multi line
 * @author Deng Yangjun
 * @date 2007-1-9
 * @version 0.2
 */
 
#ifndef INI_FILE_H_
#define INI_FILE_H_
#include <string>
std::string app_path();
#ifdef __cplusplus
extern "C"
{
#endif
#define INI_SUCCEED (0)
#define INI_ERROR (-1)
#define INI_NOT_EXIST (-2)
	
int read_profile_string(const char *profile, const char *AppName, const char *KeyName, char *KeyVal,const char* defval );
int read_profile_int(const char *profile, const char *AppName, const char *KeyName, int* KeyVal,int defval );
int write_profile_string( const char *section, const char *key,const char *value, const char *file);

#ifdef __cplusplus
}; //end of extern "C" {
#endif

#endif //end of INI_FILE_H_
