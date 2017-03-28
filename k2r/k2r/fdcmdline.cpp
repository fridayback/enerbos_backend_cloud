#include "fdcmdline.h"
#include <iostream>


// fdcmdline::fdcmdline()
// {
// }
// 
// 
// fdcmdline::~fdcmdline()
// {
// }
static string g_strhelp = "";
void init_cmdline_evn(string welcome /*= "welcome to use fd command line"*/, string help /*= "help information not exist!"*/)
{
	cout << welcome << endl<<">";
	g_strhelp = help;
}

bool get_cmdline(string &cmd)
{
	getline(cin, cmd);
	cout << ">";
	return true;
}
