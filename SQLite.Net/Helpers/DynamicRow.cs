using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Text;

namespace SQLite.Net.Helpers
{
	internal class DynamicRow : DynamicObject
	{
		private readonly IReadOnlyDictionary<string, int> _columnIndexes;
		private readonly object[] _rowValues;

		public DynamicRow(IReadOnlyDictionary<string, int> columnIndexes, SQLiteQuery query)
		{
			_columnIndexes = columnIndexes;
			_rowValues = new object[query.ColumnCount];
			for (int i = 0; i < query.ColumnCount; i++)
			{
				_rowValues[i] = query[i];
			}
		}

		public override bool TryGetIndex(GetIndexBinder binder, object[] indexes, out object result)
		{
			if (indexes.Length == 1)
			{
				if (indexes[0] is int index && 0 <= index && index < _rowValues.Length)
				{
					result = _rowValues[index];
					return true;
				}
				if (indexes[0] is string columnName && _columnIndexes.TryGetValue(columnName, out index))
				{
					result = _rowValues[index];
					return true;
				}
			}
			return base.TryGetIndex(binder, indexes, out result);
		}

		public override bool TryGetMember(GetMemberBinder binder, out object result)
		{
			if (_columnIndexes.TryGetValue(binder.Name, out var index))
			{
				result = _rowValues[index];
				return true;
			}
			return base.TryGetMember(binder, out result);
		}
	}
}
