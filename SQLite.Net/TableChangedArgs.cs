using System;
using System.Collections.Generic;
using System.Text;

namespace SQLite.Net
{
    public class TableChangedArgs : EventArgs
    {
	    internal TableChangedArgs(ChangeType type, string table, long rowId)
	    {
		    Type = type;
		    Table = table;
		    RowId = rowId;
	    }

		public ChangeType Type { get; }
		public string Table { get; }
		public long RowId { get; }
    }

	public enum ChangeType { Update, Insert, Delete }
}
