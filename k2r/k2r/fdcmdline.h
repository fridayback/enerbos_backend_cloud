#ifndef _FD_CMDLINE_H
#define _FD_CMDLINE_H
#include <string>
using namespace std;
// class fdcmdline
// {
// public:
// 	fdcmdline();
// 	virtual ~fdcmdline();
// };
void init_cmdline_evn(string welcome = "welcome to use fd command line",string help = "help information not exist!");
bool get_cmdline(string& cmd);

#endif
