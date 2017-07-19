using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NUnit.Framework;

namespace Tests
{
	[TestFixture]
    public class OtherLinqTests
    {
		[Test]
	    public void TestSingle()
	    {
		    using (var db = new SQLiteDb<string>())
		    {
			    Assert.Throws<InvalidOperationException>(() => db.DataTable.Single());

			    db.DataTable.Insert(new Data<string> { Value = "Random Text" });

			    var result = db.DataTable.Single();
				Assert.AreEqual(result.Id, 1);
				Assert.AreEqual(result.Value, "Random Text");

			    db.DataTable.Insert(new Data<string> { Value = "Random Text 2" });
			    Assert.Throws<InvalidOperationException>(() => db.DataTable.Single());

			    result = db.DataTable.Single(d => d.Id == 2);
			    Assert.AreEqual(result.Id, 2);
			    Assert.AreEqual(result.Value, "Random Text 2");

			    Assert.Throws<InvalidOperationException>(() => db.DataTable.Single(d => d.Id == 3));
			}
	    }

	    [Test]
	    public void TestSingleOrDefault()
	    {
		    using (var db = new SQLiteDb<string>())
		    {
			    Assert.IsNull(db.DataTable.SingleOrDefault());

			    db.DataTable.Insert(new Data<string>() { Value = "Random Text" });

			    var result = db.DataTable.SingleOrDefault();

			    Assert.AreEqual(result.Id, 1);
			    Assert.AreEqual(result.Value, "Random Text");

			    db.DataTable.Insert(new Data<string>() { Value = "Random Text 2" });
			    Assert.Throws<InvalidOperationException>(() => db.DataTable.SingleOrDefault());

			    result = db.DataTable.SingleOrDefault(d => d.Id == 2);
			    Assert.AreEqual(result.Id, 2);
			    Assert.AreEqual(result.Value, "Random Text 2");

			    Assert.IsNull(db.DataTable.SingleOrDefault(d => d.Id == 3));
			}
	    }

	    [Test]
	    public void TestFirst()
	    {
		    using (var db = new SQLiteDb<string>())
		    {
			    Assert.Throws<InvalidOperationException>(() => db.DataTable.First());

			    db.DataTable.Insert(new Data<string>() { Value = "Random Text" });
			    db.DataTable.Insert(new Data<string>() { Value = "Random Text 2" });

			    var result = db.DataTable.First();
			    Assert.AreEqual(result.Id, 1);
			    Assert.AreEqual(result.Value, "Random Text");
				
				result = db.DataTable.First(d => d.Id == 2);
			    Assert.AreEqual(result.Id, 2);
			    Assert.AreEqual(result.Value, "Random Text 2");

			    Assert.Throws<InvalidOperationException>(() => db.DataTable.First(d => d.Id == 3));
			}
	    }

	    [Test]
	    public void TestFirstOrDefault()
	    {
		    using (var db = new SQLiteDb<string>())
		    {
			    Assert.IsNull(db.DataTable.FirstOrDefault());

			    db.DataTable.Insert(new Data<string>() { Value = "Random Text" });
			    db.DataTable.Insert(new Data<string>() { Value = "Random Text 2" });

			    var result = db.DataTable.FirstOrDefault();
			    Assert.AreEqual(result.Id, 1);
			    Assert.AreEqual(result.Value, "Random Text");
				
			    result = db.DataTable.FirstOrDefault(d => d.Id == 2);
			    Assert.AreEqual(result.Id, 2);
			    Assert.AreEqual(result.Value, "Random Text 2");

			    Assert.IsNull(db.DataTable.FirstOrDefault(d => d.Id == 3));
			}
		}

	    [Test]
	    [Ignore("Not implemented")]
		public void TestLast()
	    {
		    using (var db = new SQLiteDb<string>())
		    {
			    Assert.Throws<InvalidOperationException>(() => db.DataTable.Last());

			    db.DataTable.Insert(new Data<string>() { Value = "Random Text" });
			    db.DataTable.Insert(new Data<string>() { Value = "Random Text 2" });

			    var result = db.DataTable.Last();
			    Assert.AreEqual(result.Id, 2);
			    Assert.AreEqual(result.Value, "Random Text 2");
				
			    result = db.DataTable.Last(d => d.Id < 2);
			    Assert.AreEqual(result.Id, 1);
			    Assert.AreEqual(result.Value, "Random Text");

			    Assert.Throws<InvalidOperationException>(() => db.DataTable.Last(d => d.Id == 3));
		    }
		}

	    [Test]
	    [Ignore("Not implemented")]
		public void TestLastOrDefault()
	    {
		    using (var db = new SQLiteDb<string>())
		    {
				Assert.IsNull(db.DataTable.LastOrDefault());

			    db.DataTable.Insert(new Data<string>() { Value = "Random Text" });
			    db.DataTable.Insert(new Data<string>() { Value = "Random Text 2" });

			    var result = db.DataTable.LastOrDefault();
			    Assert.AreEqual(result.Id, 2);
			    Assert.AreEqual(result.Value, "Random Text 2");

			    result = db.DataTable.LastOrDefault(d => d.Id < 2);
			    Assert.AreEqual(result.Id, 1);
			    Assert.AreEqual(result.Value, "Random Text");

			    Assert.IsNull(db.DataTable.LastOrDefault(d => d.Id == 3));
		    }
		}

	    [Test]
	    public void TestElementAt()
	    {
		    using (var db = new SQLiteDb<string>())
		    {
			    Assert.Throws<InvalidOperationException>(() => db.DataTable.ElementAt(0));

			    db.DataTable.Insert(new Data<string>() { Value = "Random Text" });
			    db.DataTable.Insert(new Data<string>() { Value = "Random Text 2" });

			    var result = db.DataTable.ElementAt(1);
			    Assert.AreEqual(result.Id, 2);
			    Assert.AreEqual(result.Value, "Random Text 2");

			    result = db.DataTable.ElementAt(0);
			    Assert.AreEqual(result.Id, 1);
			    Assert.AreEqual(result.Value, "Random Text");

			    Assert.Throws<InvalidOperationException>(() => db.DataTable.ElementAt(3));
		    }
	    }

	    [Test]
		public void TestElementAtOrDefault()
	    {
		    using (var db = new SQLiteDb<string>())
		    {
			    Assert.IsNull(db.DataTable.ElementAtOrDefault(0));

			    db.DataTable.Insert(new Data<string>() { Value = "Random Text" });
			    db.DataTable.Insert(new Data<string>() { Value = "Random Text 2" });

			    var result = db.DataTable.ElementAtOrDefault(1);
			    Assert.AreEqual(result.Id, 2);
			    Assert.AreEqual(result.Value, "Random Text 2");

			    result = db.DataTable.ElementAtOrDefault(0);
			    Assert.AreEqual(result.Id, 1);
			    Assert.AreEqual(result.Value, "Random Text");

			    Assert.IsNull(db.DataTable.ElementAtOrDefault(3));
		    }
		}

	    [Test]
	    public void TestAny()
	    {
		    using (var db = new SQLiteDb<string>())
		    {
				Assert.IsFalse(db.DataTable.Any());

			    db.DataTable.Insert(new Data<string>() { Value = "Random Text" });
			    db.DataTable.Insert(new Data<string>() { Value = "Random Text 2" });

				Assert.IsTrue(db.DataTable.Any());

			    Assert.IsTrue(db.DataTable.Any(d => d.Id == 1));
			    Assert.IsTrue(db.DataTable.Any(d => d.Id == 2));
			    Assert.IsFalse(db.DataTable.Any(d => d.Id == 3));

			    Assert.IsTrue(db.DataTable.Any(d => d.Value == "Random Text"));
			    Assert.IsTrue(db.DataTable.Any(d => d.Value == "Random Text 2"));
			    Assert.IsFalse(db.DataTable.Any(d => d.Value == "Other Text"));
		    }
		}

	    [Test]
	    public void TestAll()
	    {
		    using (var db = new SQLiteDb<string>())
		    {
			    Assert.IsTrue(db.DataTable.All(d => d.Id == 1));

			    db.DataTable.Insert(new Data<string>() { Value = "Random Text" });
			    db.DataTable.Insert(new Data<string>() { Value = "Random Text 2" });

			    Assert.IsFalse(db.DataTable.All(d => d.Id == 1));
			    Assert.IsFalse(db.DataTable.All(d => d.Id == 2));

				Assert.IsTrue(db.DataTable.All(d => d.Id == 1 || d.Id == 2));
			    Assert.IsTrue(db.DataTable.All(d => d.Value == "Random Text" || d.Value == "Random Text 2"));
		    }
		}

	    [Test]
	    public void TestContains()
	    {
		    using (var db = new SQLiteDb<string>())
		    {
			    db.Logger = Console.Out;

			    db.DataTable.Insert(new Data<string>() { Value = "Random Text" });
			    db.DataTable.Insert(new Data<string>() { Value = "Random Text 2" });

			    Assert.IsTrue(db.DataTable.Select(d => d.Id).Contains(1));
			    Assert.IsTrue(db.DataTable.Select(d => d.Id).Contains(2));
			    Assert.IsFalse(db.DataTable.Select(d => d.Id).Contains(3));

			    Assert.IsTrue(db.DataTable.Select(d => d.Value).Contains("Random Text"));
			    Assert.IsTrue(db.DataTable.Select(d => d.Value).Contains("Random Text 2"));
			    Assert.IsFalse(db.DataTable.Select(d => d.Value).Contains("Other Text"));
		    }
	    }
	}
}
