using System;
using System.Collections.Generic;
using System.Text;

namespace SQLite.Net
{
	/// <summary>
	/// The ON CONFLICT clause applies to UNIQUE, NOT NULL, CHECK, and PRIMARY KEY constraints.
	/// <para>The ON CONFLICT algorithm does not apply to FOREIGN KEY constraints.</para>
	/// <para>There are five conflict resolution algorithm choices: ROLLBACK, ABORT, FAIL, IGNORE, and REPLACE.</para>
	/// <para>The default conflict resolution algorithm is ABORT.</para>
	/// </summary>
	public enum Conflict
    {
		/// <summary>
		/// When an applicable constraint violation occurs,
		/// the IGNORE resolution algorithm skips the one row that contains the constraint violation
		/// and continues processing subsequent rows of the SQL statement as if nothing went wrong.
		/// <para>Other rows before and after the row that contained the constraint violation are inserted or updated normally.</para>
		/// <para>No error is returned when the IGNORE conflict resolution algorithm is used.</para>
		/// </summary>
		Ignore,
		/// <summary>
		/// When a UNIQUE or PRIMARY KEY constraint violation occurs,
		/// the REPLACE algorithm deletes pre-existing rows that are causing the constraint violation
		/// prior to inserting or updating the current row and the command continues executing normally.
		/// <para>
		/// If a NOT NULL constraint violation occurs,
		/// the REPLACE conflict resolution replaces the NULL value with the default value for that column,
		/// or if the column has no default value, then the ABORT algorithm is used.
		/// </para>
		/// <para>If a CHECK constraint violation occurs, the REPLACE conflict resolution algorithm always works like ABORT.</para>
		/// </summary>
		Replace,
		/// <summary>
		/// When an applicable constraint violation occurs,
		/// the ABORT resolution algorithm aborts the current SQL statement with an SQLITE_CONSTRAINT error
		/// and backs out any changes made by the current SQL statement;
		/// but changes caused by prior SQL statements within the same transaction are preserved and the transaction remains active.
		/// <para>This is the default behavior and the behavior specified by the SQL standard.</para>
		/// </summary>
		Abort,
		/// <summary>
		/// When an applicable constraint violation occurs,
		/// the FAIL resolution algorithm aborts the current SQL statement with an SQLITE_CONSTRAINT error.
		/// But the FAIL resolution does not back out prior changes of the SQL statement that failed nor does it end the transaction.
		/// <para>
		/// For example, if an UPDATE statement encountered a constraint violation on the 100th row that it attempts to update,
		/// then the first 99 row changes are preserved but changes to rows 100 and beyond never occur.
		/// </para>
		/// </summary>
		Fail,
		/// <summary>
		/// When an applicable constraint violation occurs,
		/// the ROLLBACK resolution algorithm aborts the current SQL statement with an SQLITE_CONSTRAINT error
		/// and rolls back the current transaction.
		/// <para>
		/// If no transaction is active (other than the implied transaction that is created on every command)
		/// then the ROLLBACK resolution algorithm works the same as the ABORT algorithm.
		/// </para>
		/// </summary>
		Rollback
	}
}
