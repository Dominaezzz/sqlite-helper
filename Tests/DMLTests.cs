using System;
using System.Linq;
using SQLite.Net;
using SQLite.Net.Attributes;
using SQLite.Net.Exceptions;
using Xunit;

namespace Tests
{
//	[TestFixture(TestName = "Data Manipulation Tests")]
    public class DMLTests : IDisposable
    {
		public class TestDb : SQLiteDatabase
		{
			public Table<Product> Products { get; set; }

			public TestDb()
			{
				if (UserVersion == 0)
				{
					CreateTable("Products", c => new
					{
						Id = c.Column<int>(primaryKey:true),
						Name = c.Column<string>(nullable:false),
						Price = c.Column<decimal>()
					},
					t => new
					{
						UniqueProductNames = t.Unique(p => p.Name)
					});
				}
				UserVersion++;
			}
		}
		[Table("Products")]
		public class Product
		{
			[PrimaryKey]
			public int Id { get; set; }
			public string Name { get; set; }
			public decimal Price { get; set; }
		}

	    private readonly TestDb _db;

	    public DMLTests()
	    {
		    _db = new TestDb();
	    }

	    public void Dispose()
	    {
		    _db.Dispose();
	    }


		[Fact]
		[Trait("Category", "Insert")]
	    public void TestInsertSimple()
	    {
		    Assert.Equal(0, _db.Products.Count());

		    int count = _db.Products.Insert(new Product { Name = "Laptop", Price = 1500M });

			Assert.Equal(1, count);
			Assert.Equal(1, _db.Products.Count());

		    var product = _db.Products.Single();

			Assert.Equal(1, product.Id);
			Assert.Equal("Laptop", product.Name);
			Assert.Equal(1500M, product.Price);
	    }

		[Fact]
		[Trait("Category", "Insert")]
		public void TestInsertBatch()
		{
			Assert.Equal(0, _db.Products.Count());

			var products = new []
			{
				new Product { Name = "Laptop", Price = 1500M },
				new Product { Name = "Computer", Price = 2000M },
				new Product { Name = "Chocolate", Price = 15M },
				new Product { Name = "Piano", Price = 2500M },
				new Product { Name = "Water", Price = 1.5M },
				new Product { Name = "Paper", Price = 0.5M },
			};

			int count = _db.Products.Insert(products);

			Assert.Equal(products.Length, count);
			Assert.Equal(products.Length, _db.Products.Count());

			int i = 0;
			foreach (var dbProduct in _db.Products)
			{
				var product = products[i++];
				Assert.Equal(i, dbProduct.Id);
				Assert.Equal(product.Name, dbProduct.Name);
				Assert.Equal(product.Price, dbProduct.Price);
			}
		}

	    [Fact]
	    [Trait("Category", "Insert")]
		public void TestInsertBreaksUniqueConstraint()
	    {
		    Assert.Equal(0, _db.Products.Count());

		    var product1 = new Product { Name = "Laptop", Price = 1500M };
		    var product2 = new Product { Name = "Laptop", Price = 2000M };

		    int count = _db.Products.Insert(product1);
		    Assert.Equal(1, count);
		    Assert.Equal(1, _db.Products.Count());

		    Assert.Throws<UniqueConstraintException>(() => _db.Products.Insert(product2));
		    Assert.Equal(1, _db.Products.Count());
		}

	    [Fact]
	    [Trait("Category", "Insert")]
		public void TestInsertOrIgnoreAgainstUniqueConstraint()
	    {
		    Assert.Equal(0, _db.Products.Count());

		    var product1 = new Product { Name = "Laptop", Price = 1500M };
		    var product2 = new Product { Name = "Laptop", Price = 2000M };

		    int count = _db.Products.Insert(product1);
		    Assert.Equal(1, count);
		    Assert.Equal(1, _db.Products.Count());

		    count = _db.Products.Insert(product2, Conflict.Ignore);
			Assert.Equal(0, count);
		    Assert.Equal(1, _db.Products.Count());
		}

	    [Fact]
	    [Trait("Category", "Insert")]
		public void TestInsertOrReplaceAgainstUniqueConstraint()
	    {
		    Assert.Equal(0, _db.Products.Count());

		    var product1 = new Product { Name = "Laptop", Price = 1500M };
		    var product2 = new Product { Name = "Laptop", Price = 2000M };

		    int count = _db.Products.Insert(product1);
		    Assert.Equal(1, count);
		    Assert.Equal(1, _db.Products.Count());
			Assert.True(_db.Products.Any(p => p.Id == 1 && p.Name == "Laptop" && p.Price == 1500M));

		    count = _db.Products.Insert(product2, Conflict.Replace);
		    Assert.Equal(1, count);
			Assert.Equal(1, _db.Products.Count());
		    Assert.True(_db.Products.Any(p => p.Id == 2 && p.Name == "Laptop" && p.Price == 2000M));
		}

	    [Fact]
	    [Trait("Category", "Insert")]
		public void TestInsertOrAbortBreaksUniqueConstraint()
	    {
		    Assert.Equal(0, _db.Products.Count());

		    var product1 = new Product { Name = "Laptop", Price = 1500M };
		    var product2 = new Product { Name = "Laptop", Price = 2000M };
		    var product3 = new Product { Name = "Computer", Price = 2300M };
			
		    Assert.Throws<UniqueConstraintException>(() => _db.Products.Insert(new[] {product1, product2, product3}, Conflict.Abort));
		    Assert.Equal(0, _db.Products.Count());
		}

	    [Fact]
	    [Trait("Category", "Insert")]
		public void TestInsertOrFailBreaksUniqueConstraint()
	    {
		    Assert.Equal(0, _db.Products.Count());

		    var product1 = new Product { Name = "Laptop", Price = 1500M };
		    var product2 = new Product { Name = "Laptop", Price = 2000M };

		    int count = _db.Products.Insert(product1);
		    Assert.Equal(1, count);
		    Assert.Equal(1, _db.Products.Count());

		    Assert.Throws<UniqueConstraintException>(() => _db.Products.Insert(product2, Conflict.Fail));
		    Assert.Equal(1, _db.Products.Count());
		}

	    [Fact]
	    [Trait("Category", "Insert")]
		public void TestInsertOrRollBackBreaksUniqueConstraint()
	    {
		    Assert.Equal(0, _db.Products.Count());

		    var product1 = new Product { Name = "Laptop", Price = 1500M };
		    var product2 = new Product { Name = "Laptop", Price = 2000M };
		    var product3 = new Product { Name = "Computer", Price = 2300M };

			_db.BeginTransaction();
			{
				Assert.Equal(1, _db.Products.Insert(product1, Conflict.Rollback));
				Assert.Throws<UniqueConstraintException>(() => _db.Products.Insert(product2, Conflict.Rollback));
				Assert.Equal(1, _db.Products.Insert(product3, Conflict.Rollback));
			}
			Assert.Throws<SQLiteException>(() => _db.EndTransaction());
			
		    Assert.Equal(1, _db.Products.Count());
		    Assert.True(_db.Products.Any(p => p.Id == 1 && p.Name == "Computer" && p.Price == 2300M));
	    }


		[Fact]
		[Trait("Category", "Delete")]
	    public void TestDeleteAll()
	    {
			Assert.Equal(0, _db.Products.Count());

		    var products = new[]
		    {
			    new Product { Name = "Laptop", Price = 1500M },
			    new Product { Name = "Computer", Price = 2000M },
			    new Product { Name = "Chocolate", Price = 15M },
			    new Product { Name = "Piano", Price = 2500M },
			    new Product { Name = "Water", Price = 1.5M },
			    new Product { Name = "Paper", Price = 0.5M },
		    };

		    _db.Products.Insert(products);
		    Assert.Equal(products.Length, _db.Products.Count());

		    int count = _db.Products.DeleteAll();
			Assert.Equal(products.Length, count);
			Assert.Equal(0, _db.Products.Count());
	    }

	    [Fact]
	    [Trait("Category", "Delete")]
	    public void TestDelete()
	    {
		    Assert.Equal(0, _db.Products.Count());

		    var products = new[]
		    {
			    new Product { Name = "Laptop", Price = 1500M },
			    new Product { Name = "Computer", Price = 2000M },
			    new Product { Name = "Chocolate", Price = 15M },
			    new Product { Name = "Piano", Price = 2500M },
			    new Product { Name = "Water", Price = 1.5M },
			    new Product { Name = "Paper", Price = 0.5M },
		    };

		    int count = _db.Products.Insert(products);

		    Assert.Equal(products.Length, count);
		    Assert.Equal(products.Length, _db.Products.Count());

		    _db.Products.Delete(p => p.Id == 2);

			Assert.Equal(products.Length - 1, _db.Products.Count());
		}

	    [Fact]
	    [Trait("Category", "Delete")]
	    public void TestDeleteMultiple()
	    {
		    Assert.Equal(0, _db.Products.Count());

		    var products = new[]
		    {
			    new Product { Name = "Laptop", Price = 1500M },
			    new Product { Name = "Computer", Price = 2000M },
			    new Product { Name = "Chocolate", Price = 15M },
			    new Product { Name = "Piano", Price = 2500M },
			    new Product { Name = "Water", Price = 1.5M },
			    new Product { Name = "Paper", Price = 0.5M },
		    };

		    int count = _db.Products.Insert(products);

		    Assert.Equal(products.Length, count);
		    Assert.Equal(products.Length, _db.Products.Count());

		    _db.Products.Delete(p => p.Price > 1000);

		    Assert.Equal(products.Length - 3, _db.Products.Count());
		}


	    [Fact]
	    [Trait("Category", "Update")]
	    public void TestUpdate()
	    {
		    Assert.Equal(0, _db.Products.Count());

		    var products = new[]
		    {
			    new Product { Name = "Laptop", Price = 1500M },
			    new Product { Name = "Computer", Price = 2000M },
			    new Product { Name = "Chocolate", Price = 15M },
			    new Product { Name = "Piano", Price = 2500M },
			    new Product { Name = "Water", Price = 1.5M },
			    new Product { Name = "Paper", Price = 0.5M },
		    };

		    Assert.Equal(products.Length, _db.Products.Insert(products));
		    Assert.Equal(products.Length, _db.Products.Count());

			Assert.True(_db.Products.Any(p => p.Id == 4 && p.Name == "Piano" && p.Price == 2500));

		    var product = _db.Products.Single(p => p.Id == 4);
		    product.Price = 1234;
			
			Assert.Equal(1, _db.Products.Update(product));

		    Assert.True(_db.Products.Any(p => p.Id == 4 && p.Name == "Piano" && p.Price == 1234));
		}
	}
}
