using System;
using System.Collections.Generic;
using System.Text;
using SQLite.Net.Attributes;

namespace SQLite.Net
{
    public class DatabaseObject
    {
		/// <summary>
		/// The type of this database object.
		/// One of table, index, trigger and view.
		/// </summary>
		[Column("type")]
		public string Type { get; set; }

		/// <summary>
		/// The name of this database object.
		/// </summary>
		[Column("name")]
		public string Name { get; set; }

		/// <summary>
		/// The name of the database table that this database object affects.
		/// </summary>
		[Column("tbl_name")]
		public string TableName { get; set; }

		[Column("rootpage")]
		public int RootPage { get; set; }

		/// <summary>
		/// The SQL statement used to create this database object.
		/// For implicitly created indices then is null. e.g. PRIMARY KEY or UNIQUE constraints.
		/// </summary>
		[Column("sql")]
		public string SQL { get; set; }
    }
}
