using System;
using System.Collections.Generic;
using System.Text;
using SQLite.Net.Exceptions;
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
		    switch (raw.sqlite3_step(_stmt))
		    {
				case raw.SQLITE_DONE:
					Reset();
					break;
				case raw.SQLITE_ERROR:
					string errorMessage = raw.sqlite3_errmsg(Db);
					Reset();
					throw new SQLiteException(errorMessage);
				case raw.SQLITE_CONSTRAINT:
					string message = raw.sqlite3_errmsg(Db);
					int errCode = raw.sqlite3_extended_errcode(Db);
					Reset();
					switch (errCode)
					{
						case raw.SQLITE_CONSTRAINT_NOTNULL:
							throw new NotNullConstraintException(message);
						case raw.SQLITE_CONSTRAINT_UNIQUE:
							throw new UniqueConstraintException(message);
						case raw.SQLITE_CONSTRAINT_FOREIGNKEY:
							throw new ForeignKeyConstraintException(message);
						case raw.SQLITE_CONSTRAINT_PRIMARYKEY:
							throw new PrimaryKeyConstraintException(message);
						case raw.SQLITE_CONSTRAINT_CHECK:
							throw new CheckConstraintException(message);
					}
					throw new SQLiteConstraintException(message);
				default:
					Reset();
					throw new SQLiteException("Could not execute statement.");
		    }
	    }
    }
}
