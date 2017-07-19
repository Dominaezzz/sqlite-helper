using System;
using System.Collections.Generic;
using System.Text;

namespace SQLite.Net
{
    public enum OnAction
    {
		SetNull,
		SetDefault,
		Cascade,
		Restrict,
		NoAction
    }
}
