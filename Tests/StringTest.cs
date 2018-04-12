using System;
using System.Linq;
using Xunit;

namespace Tests
{
	[Trait("Category", "Strings")]
    public class StringTest : IDisposable
	{
		private readonly SQLiteDb<string> _db;
		private const string TestString = "ABCabc123";
		private const string TestStringLower = "abcabc123";
		private const string TestStringUpper = "ABCABC123";

		public StringTest()
		{
			_db = new SQLiteDb<string>();
			_db.DataTable.Insert(new Data<string> { Value = TestString });
		}

		public void Dispose()
		{
			_db.Dispose();
		}


		[Fact]
		public void TestLength()
		{
			Assert.Equal(TestString.Length, _db.DataTable.Select(d => d.Value.Length).Single());
		}

		[Fact]
		public void TestToLower()
		{
			Assert.Equal(TestString.ToLower(), _db.DataTable.Select(d => d.Value.ToLower()).Single());
		}

		[Fact]
		public void TestToUpper()
		{
			Assert.Equal(TestString.ToUpper(), _db.DataTable.Select(d => d.Value.ToUpper()).Single());
		}

		[Fact]
		public void TestTrim()
		{
			Assert.Equal(TestString.Trim(), _db.DataTable.Select(d => d.Value.Trim()).Single());
		}

		[Fact]
		public void TestTrimStart()
		{
			Assert.Equal(TestString.TrimStart(), _db.DataTable.Select(d => d.Value.TrimStart()).Single());
		}

		[Fact]
		public void TestTrimEnd()
		{
			Assert.Equal(TestString.TrimEnd(), _db.DataTable.Select(d => d.Value.TrimEnd()).Single());
		}

		[Theory]
		[InlineData(TestString)]
		[InlineData(TestStringLower)]
		[InlineData(TestStringUpper)]
		public void TestEquals(string value)
		{
			Assert.Equal(TestString == value, _db.DataTable.Select(d => d.Value == value).Single());
		}

		[Theory]
		[InlineData(TestString)]
		[InlineData(TestStringLower)]
		[InlineData(TestStringUpper)]
		public void TestNotEquals(string value)
		{
			Assert.Equal(TestString != value, _db.DataTable.Select(d => d.Value != value).Single());
		}

		[Theory]
		[InlineData(TestString)]
		[InlineData(TestStringLower)]
		[InlineData(TestStringUpper)]
		public void TestStartsWith(string value)
		{
			Assert.Equal(TestString.StartsWith(value), _db.DataTable.Select(d => d.Value.StartsWith(value)).Single());
		}

		[Theory]
		[InlineData(TestString)]
		[InlineData(TestStringLower)]
		[InlineData(TestStringUpper)]
		public void TestEndsWith(string value)
		{
			Assert.Equal(TestString.EndsWith(value), _db.DataTable.Select(d => d.Value.EndsWith(value)).Single());
		}

		[Theory]
		[InlineData(TestString)]
		[InlineData(TestStringLower)]
		[InlineData(TestStringUpper)]
		public void TestContains(string value)
		{
			Assert.Equal(TestString.Contains(value), _db.DataTable.Select(d => d.Value.Contains(value)).Single());
		}

		[Theory]
		[InlineData(TestString)]
		[InlineData(TestStringLower)]
		[InlineData(TestStringUpper)]
		public void TestReplace(string value)
		{
			Assert.Equal(TestString.Replace('a', 'b'), _db.DataTable.Select(d => d.Value.Replace('a', 'b')).Single());
		}

		[Theory]
		[InlineData(0)]
		[InlineData(3)]
		[InlineData(6)]
		public void TestRemove(int index)
		{
			Assert.Equal(TestString.Remove(index), _db.DataTable.Select(d => d.Value.Remove(index)).Single());
		}
	}
}
