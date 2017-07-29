using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NUnit.Framework;

namespace Tests
{
	[TestFixture(Category = "Strings")]
    public class StringTest
	{
		private SQLiteDb<string> _db;
		private const string TestString = "ABCabc123";
		private const string TestStringLower = "abcabc123";
		private const string TestStringUpper = "ABCABC123";

		[OneTimeSetUp]
		public void TestSetUp()
		{
			_db = new SQLiteDb<string>();
			_db.DataTable.Insert(new Data<string> { Value = TestString });

			_db.Log = Console.WriteLine;
		}

		[OneTimeTearDown]
		public void TestTearDown()
		{
			_db.Dispose();
		}


		[Test]
		public void TestLength()
		{
			Assert.AreEqual(TestString.Length, _db.DataTable.Select(d => d.Value.Length).Single());
		}

		[Test]
		public void TestToLower()
		{
			Assert.AreEqual(TestString.ToLower(), _db.DataTable.Select(d => d.Value.ToLower()).Single());
		}

		[Test]
		public void TestToUpper()
		{
			Assert.AreEqual(TestString.ToUpper(), _db.DataTable.Select(d => d.Value.ToUpper()).Single());
		}

		[Test]
		public void TestTrim()
		{
			Assert.AreEqual(TestString.Trim(), _db.DataTable.Select(d => d.Value.Trim()).Single());
		}

		[Test]
		public void TestTrimStart()
		{
			Assert.AreEqual(TestString.TrimStart(), _db.DataTable.Select(d => d.Value.TrimEnd()).Single());
		}

		[Test]
		public void TestTrimEnd()
		{
			Assert.AreEqual(TestString.TrimEnd(), _db.DataTable.Select(d => d.Value.TrimEnd()).Single());
		}

		[Test]
		[TestCase(TestString)]
		[TestCase(TestStringLower)]
		[TestCase(TestStringUpper)]
		public void TestEquals(string value)
		{
			Assert.AreEqual(TestString == value, _db.DataTable.Select(d => d.Value == value).Single());
		}

		[Test]
		[TestCase(TestString)]
		[TestCase(TestStringLower)]
		[TestCase(TestStringUpper)]
		public void TestNotEquals(string value)
		{
			Assert.AreEqual(TestString != value, _db.DataTable.Select(d => d.Value != value).Single());
		}

		[Test]
		[TestCase(TestString)]
		[TestCase(TestStringLower)]
		[TestCase(TestStringUpper)]
		public void TestStartsWith(string value)
		{
			Assert.AreEqual(TestString.StartsWith(value), _db.DataTable.Select(d => d.Value.StartsWith(value)).Single());
		}

		[Test]
		[TestCase(TestString)]
		[TestCase(TestStringLower)]
		[TestCase(TestStringUpper)]
		public void TestEndsWith(string value)
		{
			Assert.AreEqual(TestString.EndsWith(value), _db.DataTable.Select(d => d.Value.EndsWith(value)).Single());
		}

		[Test]
		[TestCase(TestString)]
		[TestCase(TestStringLower)]
		[TestCase(TestStringUpper)]
		public void TestContains(string value)
		{
			Assert.AreEqual(TestString.Contains(value), _db.DataTable.Select(d => d.Value.Contains(value)).Single());
		}

		[Test]
		[TestCase(TestString)]
		[TestCase(TestStringLower)]
		[TestCase(TestStringUpper)]
		public void TestReplace(string value)
		{
			Assert.AreEqual(TestString == value, _db.DataTable.Select(d => d.Value == value).Single());
		}

		[Test]
		[TestCase(0)]
		[TestCase(3)]
		[TestCase(6)]
		public void TestRemove(int index)
		{
			Assert.AreEqual(TestString.Remove(index), _db.DataTable.Select(d => d.Value.Remove(index)).Single());
		}
	}
}
