/**
 * @file
 * @brief initialization file read and write API implementation
 * @author Deng Yangjun
 * @date 2007-1-9
 * @version 0.2
 */
#if 1
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>
#include <ctype.h>
#include <string>
#ifdef _WIN32
#include <Windows.h>
#else
#endif

#include "inifile.h"

std::string app_path()
{
#ifdef _WIN32
	TCHAR szFilePath[MAX_PATH + 1];
	GetModuleFileName(NULL, szFilePath, MAX_PATH);
	std::string _exeName = szFilePath;
	_exeName = _exeName.substr(0,_exeName.rfind("\\"));
	return _exeName;
#else
	std::string _exeName = "/proc/self/exe";
	size_t linksize = 256;
	char   exeName[256] = {0};
	if(readlink(_exeName.c_str() , exeName, linksize) !=-1 )
	{
		_exeName = exeName;
	}
	_exeName = _exeName.substr(0,_exeName.rfind("/"));
	_exeName = _exeName.substr(0,_exeName.rfind("/"));

	return _exeName;
#endif
}

#ifdef __cplusplus
extern "C"
{
#endif

	
#define KEYVALLEN 256
#define MAX_FILE_SIZE 1024*16

/*   删除左边的空格   */
char * l_trim(char * szOutput, const char *szInput)
{
	assert(szInput != NULL);
	assert(szOutput != NULL);
	assert(szOutput != szInput);
	for(; *szInput != '\0' && isspace(*szInput); ++szInput){;}
	return strcpy(szOutput, szInput);
}

/*   删除右边的空格   */
char *r_trim(char *szOutput, const char *szInput)
{
	char *p = NULL;
	assert(szInput != NULL);
	assert(szOutput != NULL);
	assert(szOutput != szInput);
	strcpy(szOutput, szInput);
	for(p = szOutput + strlen(szOutput) - 1; p >= szOutput && isspace(*p); --p){;}
	*(++p) = '\0';
	return szOutput;
}

/*   删除两边的空格   */
char * a_trim(char * szOutput, const char * szInput)
{
	char *p = NULL;
	assert(szInput != NULL);
	assert(szOutput != NULL);
	l_trim(szOutput, szInput);
	for   (p = szOutput + strlen(szOutput) - 1;p >= szOutput && isspace(*p); --p){;}
	*(++p) = '\0';
	return szOutput;
}

int read_profile_string(const char *profile, const char *AppName, const char *KeyName, char *KeyVal,const char* defval )
{
	char appname[32],keyname[32];
	char *buf,*c;
	char buf_i[KEYVALLEN], buf_o[KEYVALLEN];
	FILE *fp;
	int found=0; /* 1 AppName 2 KeyName */
	if( (fp=fopen( profile,"r" ))==NULL ){return INI_ERROR;}
	fseek( fp, 0, SEEK_SET );
	memset( appname, 0, sizeof(appname) );
	sprintf( appname,"[%s]", AppName );

	while( !feof(fp) && fgets( buf_i, KEYVALLEN, fp )!=NULL )
	{
		l_trim(buf_o, buf_i);
		if( strlen(buf_o) <= 0 ) continue;
		buf = NULL;
		buf = buf_o;

		if( found == 0 )
		{
			if( buf[0] != '[' ) {continue;}
			else if ( strncmp(buf,appname,strlen(appname))==0 )
			{
				found = 1;
				continue;
			}
		}
		else if( found == 1 )
		{
			if( buf[0] == '#' ){continue;}
			else if ( buf[0] == '[' ) {break;}
			else
			{
				if( (c = (char*)strchr(buf, '=')) == NULL ) continue;
				memset( keyname, 0, sizeof(keyname) );
				sscanf( buf, "%[^=|^ |^\t]", keyname );
				if( strcmp(keyname, KeyName) == 0 )
				{
					sscanf(++c,"%[^\n]",KeyVal);
					char *KeyVal_o = (char*)(malloc(strlen(KeyVal) + 1));
					if(KeyVal_o != NULL)
					{
						memset(KeyVal_o, 0, strlen(KeyVal) + 1);
						a_trim(KeyVal_o, KeyVal);
						if(KeyVal_o && strlen(KeyVal_o) > 0)
							strcpy(KeyVal, KeyVal_o);
						free(KeyVal_o);
						KeyVal_o = NULL;
					}
					found = 2;
					break;
				} else {continue;}
			}
		}
	}
	fclose( fp );
	if( found != 2 )
	{
		strncpy(KeyVal,defval,sizeof(defval));
		return INI_NOT_EXIST;
	}
	return INI_SUCCEED;
}

int read_profile_int(const char *profile, const char *AppName, const char *KeyName, int* KeyVal,int defval)
{
	char valuetmp[20] = "";
	int ret = read_profile_string(profile,AppName,KeyName,valuetmp,"");
	if(INI_ERROR == ret)
	{
		return INI_ERROR;
	}
	if(0 == valuetmp[0])
	{
		*KeyVal = defval;
	}
	else
	{
		*KeyVal = atoi(valuetmp);
	}
	return ret;
}


/**
 * @brief write a profile string to a ini file
 * @param section [in] name of the section,can't be NULL and empty string
 * @param key [in] name of the key pairs to value, can't be NULL and empty string
 * @param value [in] profile string value
 * @param file [in] path of ini file
 * @return 1 : success\n 0 : failure
 */
int write_profile_string(const char *section, const char *key,
					const char *value, const char *file)
{
	FILE* fp = NULL;
	if((fp=fopen( file,"r" ))==NULL ){return INI_ERROR;}
	char buf[MAX_FILE_SIZE] = "";
	char buf_in[MAX_FILE_SIZE] = "";
	int i = 0;
	buf[i]=fgetc(fp);
	while( buf[i]!= (char)EOF) {
		i++;
		assert( i < MAX_FILE_SIZE ); //file too big, you can redefine MAX_FILE_SIZE to fit the big file
		buf[i]=fgetc(fp);
	}
	fclose(fp);
	buf[i] = 0;

	const char *d = "\n\r";
	char *p;
	p = strtok(buf,d);
	int found = 0;
	int index = 0;
	while(p)
	{
		char tmp[256] = "";
		sprintf(tmp,"[%s]",section);
		int i = 0;
		while(' ' == p[i])
		{
			i++;
		}
		if(strcmp(p,tmp) == 0)
		{
			found = 1;
		}
		if(1 == found && strncmp(&(p[i]),key,strlen(key)) == 0 && '#' != p[0] )
		{
			sprintf(tmp,"%s=%s\n",key,value);
		}
		else
		{
			sprintf(tmp,"%s\n",p);
		}

		sprintf(&(buf_in[index]),"%s",tmp);
		index +=  strlen(tmp);
		p = strtok(NULL,d);
	}
	if( (fp=fopen( file,"w" ))==NULL ){return INI_ERROR;}
	fwrite(buf_in,strlen(buf_in),1,fp);
	fclose(fp);
	return 1;
}



#ifdef __cplusplus
}; //end of extern "C" {
#endif

#endif