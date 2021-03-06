﻿using System;
using System.Linq;
using SQLite.Net.Attributes;
using Xunit;

namespace Tests
{
    public class OtherLinqTests
    {
		[Fact]
	    public void TestSingle()
	    {
		    using (var db = new SQLiteDb<string>())
		    {
			    Assert.Throws<InvalidOperationException>(() => db.DataTable.Single());

			    db.DataTable.Insert(new Data<string> { Value = "Random Text" });

			    var result = db.DataTable.Single();
				Assert.Equal(1, result.Id);
				Assert.Equal("Random Text", result.Value);

			    db.DataTable.Insert(new Data<string> { Value = "Random Text 2" });
			    Assert.Throws<InvalidOperationException>(() => db.DataTable.Single());

			    result = db.DataTable.Single(d => d.Id == 2);
			    Assert.Equal(2, result.Id);
			    Assert.Equal("Random Text 2", result.Value);

			    Assert.Throws<InvalidOperationException>(() => db.DataTable.Single(d => d.Id == 3));
			}
	    }

	    [Fact]
	    public void TestSingleOrDefault()
	    {
		    using (var db = new SQLiteDb<string>())
		    {
			    Assert.Null(db.DataTable.SingleOrDefault());

			    db.DataTable.Insert(new Data<string>() { Value = "Random Text" });

			    var result = db.DataTable.SingleOrDefault();
				Assert.NotNull(result);
			    Assert.Equal(1, result.Id);
			    Assert.Equal("Random Text", result.Value);

			    db.DataTable.Insert(new Data<string>() { Value = "Random Text 2" });
			    Assert.Throws<InvalidOperationException>(() => db.DataTable.SingleOrDefault());

			    result = db.DataTable.SingleOrDefault(d => d.Id == 2);
				Assert.NotNull(result);
			    Assert.Equal(2, result.Id);
			    Assert.Equal("Random Text 2", result.Value);

			    Assert.Null(db.DataTable.SingleOrDefault(d => d.Id == 3));
			}
	    }

	    [Fact]
	    public void TestFirst()
	    {
		    using (var db = new SQLiteDb<string>())
		    {
			    Assert.Throws<InvalidOperationException>(() => db.DataTable.First());

			    db.DataTable.Insert(new Data<string>() { Value = "Random Text" });
			    db.DataTable.Insert(new Data<string>() { Value = "Random Text 2" });

			    var result = db.DataTable.First();
			    Assert.Equal(1, result.Id);
			    Assert.Equal("Random Text", result.Value);
				
				result = db.DataTable.First(d => d.Id == 2);
			    Assert.Equal(2, result.Id);
			    Assert.Equal("Random Text 2", result.Value);

			    Assert.Throws<InvalidOperationException>(() => db.DataTable.First(d => d.Id == 3));
			}
	    }

	    [Fact]
	    public void TestFirstOrDefault()
	    {
		    using (var db = new SQLiteDb<string>())
		    {
			    Assert.Null(db.DataTable.FirstOrDefault());

			    db.DataTable.Insert(new Data<string>() { Value = "Random Text" });
			    db.DataTable.Insert(new Data<string>() { Value = "Random Text 2" });

			    var result = db.DataTable.FirstOrDefault();
				Assert.NotNull(result);
			    Assert.Equal(1, result.Id);
			    Assert.Equal("Random Text", result.Value);
				
			    result = db.DataTable.FirstOrDefault(d => d.Id == 2);
				Assert.NotNull(result);
			    Assert.Equal(2, result.Id);
			    Assert.Equal("Random Text 2", result.Value);

			    Assert.Null(db.DataTable.FirstOrDefault(d => d.Id == 3));
			}
		}

	    [Fact(Skip = "Not implemented")]
		public void TestLast()
	    {
		    using (var db = new SQLiteDb<string>())
		    {
			    Assert.Throws<InvalidOperationException>(() => db.DataTable.Last());

			    db.DataTable.Insert(new Data<string>() { Value = "Random Text" });
			    db.DataTable.Insert(new Data<string>() { Value = "Random Text 2" });

			    var result = db.DataTable.Last();
			    Assert.Equal(2, result.Id);
			    Assert.Equal("Random Text 2", result.Value);
				
			    result = db.DataTable.Last(d => d.Id < 2);
			    Assert.Equal(1, result.Id);
			    Assert.Equal("Random Text", result.Value);

			    Assert.Throws<InvalidOperationException>(() => db.DataTable.Last(d => d.Id == 3));
		    }
		}

	    [Fact(Skip = "Not implemented")]
		public void TestLastOrDefault()
	    {
		    using (var db = new SQLiteDb<string>())
		    {
				Assert.Null(db.DataTable.LastOrDefault());

			    db.DataTable.Insert(new Data<string>() { Value = "Random Text" });
			    db.DataTable.Insert(new Data<string>() { Value = "Random Text 2" });

			    var result = db.DataTable.LastOrDefault();
				Assert.NotNull(result);
			    Assert.Equal(2, result.Id);
			    Assert.Equal("Random Text 2", result.Value);

			    result = db.DataTable.LastOrDefault(d => d.Id < 2);
				Assert.NotNull(result);
			    Assert.Equal(1, result.Id);
			    Assert.Equal("Random Text", result.Value);

			    Assert.Null(db.DataTable.LastOrDefault(d => d.Id == 3));
		    }
		}

	    [Fact]
	    public void TestElementAt()
	    {
		    using (var db = new SQLiteDb<string>())
		    {
			    Assert.Throws<InvalidOperationException>(() => db.DataTable.ElementAt(0));

			    db.DataTable.Insert(new Data<string>() { Value = "Random Text" });
			    db.DataTable.Insert(new Data<string>() { Value = "Random Text 2" });

			    var result = db.DataTable.ElementAt(1);
			    Assert.Equal(2, result.Id);
			    Assert.Equal("Random Text 2", result.Value);

			    result = db.DataTable.ElementAt(0);
			    Assert.Equal(1, result.Id);
			    Assert.Equal("Random Text", result.Value);

			    Assert.Throws<InvalidOperationException>(() => db.DataTable.ElementAt(3));
		    }
	    }

	    [Fact]
		public void TestElementAtOrDefault()
	    {
		    using (var db = new SQLiteDb<string>())
		    {
			    Assert.Null(db.DataTable.ElementAtOrDefault(0));

			    db.DataTable.Insert(new Data<string>() { Value = "Random Text" });
			    db.DataTable.Insert(new Data<string>() { Value = "Random Text 2" });

			    var result = db.DataTable.ElementAtOrDefault(1);
				Assert.NotNull(result);
			    Assert.Equal(2, result.Id);
			    Assert.Equal("Random Text 2", result.Value);

			    result = db.DataTable.ElementAtOrDefault(0);
				Assert.NotNull(result);
			    Assert.Equal(1, result.Id);
			    Assert.Equal("Random Text", result.Value);

			    Assert.Null(db.DataTable.ElementAtOrDefault(3));
		    }
		}

	    [Fact]
	    public void TestAny()
	    {
		    using (var db = new SQLiteDb<string>())
		    {
				Assert.False(db.DataTable.Any());

			    db.DataTable.Insert(new Data<string>() { Value = "Random Text" });
			    db.DataTable.Insert(new Data<string>() { Value = "Random Text 2" });

				Assert.True(db.DataTable.Any());

			    Assert.True(db.DataTable.Any(d => d.Id == 1));
			    Assert.True(db.DataTable.Any(d => d.Id == 2));
			    Assert.False(db.DataTable.Any(d => d.Id == 3));

			    Assert.True(db.DataTable.Any(d => d.Value == "Random Text"));
			    Assert.True(db.DataTable.Any(d => d.Value == "Random Text 2"));
			    Assert.False(db.DataTable.Any(d => d.Value == "Other Text"));
		    }
		}

	    [Fact]
	    public void TestAll()
	    {
		    using (var db = new SQLiteDb<string>())
		    {
			    Assert.True(db.DataTable.All(d => d.Id == 1));

			    db.DataTable.Insert(new Data<string>() { Value = "Random Text" });
			    db.DataTable.Insert(new Data<string>() { Value = "Random Text 2" });

			    Assert.False(db.DataTable.All(d => d.Id == 1));
			    Assert.False(db.DataTable.All(d => d.Id == 2));

				Assert.True(db.DataTable.All(d => d.Id == 1 || d.Id == 2));
			    Assert.True(db.DataTable.All(d => d.Value == "Random Text" || d.Value == "Random Text 2"));
		    }
		}

	    [Fact]
	    public void TestContains()
	    {
		    using (var db = new SQLiteDb<string>())
		    {
			    db.DataTable.Insert(new Data<string>() { Value = "Random Text" });
			    db.DataTable.Insert(new Data<string>() { Value = "Random Text 2" });

			    Assert.True(db.DataTable.Select(d => d.Id).Contains(1));
			    Assert.True(db.DataTable.Select(d => d.Id).Contains(2));
			    Assert.False(db.DataTable.Select(d => d.Id).Contains(3));

			    Assert.True(db.DataTable.Select(d => d.Value).Contains("Random Text"));
			    Assert.True(db.DataTable.Select(d => d.Value).Contains("Random Text 2"));
			    Assert.False(db.DataTable.Select(d => d.Value).Contains("Other Text"));
		    }
	    }
	}
}
