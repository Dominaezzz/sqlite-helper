using System;
using System.Collections.Generic;
using System.Text;
using SQLitePCL;

namespace SQLite.Net
{
    public class SQLiteStatement : SQLiteProgram
    {
	    internal SQLiteStatement(sqlite3 db, string sql) : base(db, sql)
	    {

	    }

	    public void Execute()
	    {
		    Step();
	    }
    }
}
