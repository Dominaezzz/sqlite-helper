using System;
using System.Collections.Generic;
using System.Text;

namespace SQLite.Net
{
    public enum Conflict
    {
		Ignore,
		Replace,
		Abort,
		Fail,
		Rollback
    }
}
