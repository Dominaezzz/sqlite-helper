using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NUnit.Framework;
using SQLite.Net;
using SQLite.Net.Attributes;

namespace Tests
{
	[TestFixture(TestName = "Data Manipulation Tests")]
    public class DMLTests
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
		public class Product
		{
			[PrimaryKey]
			public int Id { get; set; }
			public string Name { get; set; }
			public decimal Price { get; set; }
		}

	    private TestDb _db;

		[SetUp]
	    public void TestSetUp()
	    {
		    _db = new TestDb();
	    }

		[TearDown]
	    public void TestTearDown()
	    {
		    _db.Dispose();
	    }


		[Test]
		[Category("Insert")]
	    public void TestInsertSimple()
	    {
		    Assert.AreEqual(0, _db.Products.Count());

		    int count = _db.Products.Insert(new Product { Name = "Laptop", Price = 1500M });

			Assert.AreEqual(1, count);
			Assert.AreEqual(1, _db.Products.Count());

		    var product = _db.Products.Single();

			Assert.AreEqual(1, product.Id);
			Assert.AreEqual("Laptop", product.Name);
			Assert.AreEqual(1500M, product.Price);
	    }

		[Test]
		[Category("Insert")]
		public void TestInsertBatch()
		{
			Assert.AreEqual(0, _db.Products.Count());

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

			Assert.AreEqual(products.Length, count);
			Assert.AreEqual(products.Length, _db.Products.Count());

			int i = 0;
			foreach (var dbProduct in _db.Products)
			{
				var product = products[i++];
				Assert.AreEqual(i, dbProduct.Id);
				Assert.AreEqual(product.Name, dbProduct.Name);
				Assert.AreEqual(product.Price, dbProduct.Price);
			}
		}

	    [Test]
	    [Category("Insert")]
		public void TestInsertBreaksUniqueConstraint()
	    {
		    Assert.AreEqual(0, _db.Products.Count());

		    var product1 = new Product { Name = "Laptop", Price = 1500M };
		    var product2 = new Product { Name = "Laptop", Price = 2000M };

		    int count = _db.Products.Insert(product1);
		    Assert.AreEqual(1, count);
		    Assert.AreEqual(1, _db.Products.Count());

		    Assert.Throws<SQLiteException>(() => _db.Products.Insert(product2));
		    Assert.AreEqual(1, _db.Products.Count());
		}

	    [Test]
	    [Category("Insert")]
		public void TestInsertOrIgnoreAgainstUniqueConstraint()
	    {
		    Assert.AreEqual(0, _db.Products.Count());

		    var product1 = new Product { Name = "Laptop", Price = 1500M };
		    var product2 = new Product { Name = "Laptop", Price = 2000M };

		    int count = _db.Products.Insert(product1);
		    Assert.AreEqual(1, count);
		    Assert.AreEqual(1, _db.Products.Count());

		    count = _db.Products.Insert(product2, Conflict.Ignore);
			Assert.AreEqual(0, count);
		    Assert.AreEqual(1, _db.Products.Count());
		}

	    [Test]
	    [Category("Insert")]
		public void TestInsertOrReplaceAgainstUniqueConstraint()
	    {
		    Assert.AreEqual(0, _db.Products.Count());

		    var product1 = new Product { Name = "Laptop", Price = 1500M };
		    var product2 = new Product { Name = "Laptop", Price = 2000M };

		    int count = _db.Products.Insert(product1);
		    Assert.AreEqual(1, count);
		    Assert.AreEqual(1, _db.Products.Count());
			Assert.IsTrue(_db.Products.Any(p => p.Id == 1 && p.Name == "Laptop" && p.Price == 1500M));

		    count = _db.Products.Insert(product2, Conflict.Replace);
		    Assert.AreEqual(1, count);
			Assert.AreEqual(1, _db.Products.Count());
		    Assert.IsTrue(_db.Products.Any(p => p.Id == 2 && p.Name == "Laptop" && p.Price == 2000M));
		}

	    [Test]
	    [Category("Insert")]
		public void TestInsertOrAbortBreaksUniqueConstraint()
	    {
		    Assert.AreEqual(0, _db.Products.Count());

		    var product1 = new Product { Name = "Laptop", Price = 1500M };
		    var product2 = new Product { Name = "Laptop", Price = 2000M };
		    var product3 = new Product { Name = "Computer", Price = 2300M };
			
		    Assert.Throws<SQLiteException>(() => _db.Products.Insert(new[] {product1, product2, product3}, Conflict.Abort));
		    Assert.AreEqual(1, _db.Products.Count());
		    Assert.IsTrue(_db.Products.Any(p => p.Id == 1 && p.Name == "Laptop" && p.Price == 1500M));
		}

	    [Test]
	    [Category("Insert")]
		public void TestInsertOrFailBreaksUniqueConstraint()
	    {
		    Assert.AreEqual(0, _db.Products.Count());

		    var product1 = new Product { Name = "Laptop", Price = 1500M };
		    var product2 = new Product { Name = "Laptop", Price = 2000M };

		    int count = _db.Products.Insert(product1);
		    Assert.AreEqual(1, count);
		    Assert.AreEqual(1, _db.Products.Count());

		    Assert.Throws<SQLiteException>(() => _db.Products.Insert(product2, Conflict.Fail));
		    Assert.AreEqual(1, _db.Products.Count());
		}

	    [Test]
	    [Category("Insert")]
		public void TestInsertOrRollBackBreaksUniqueConstraint()
	    {
		    Assert.AreEqual(0, _db.Products.Count());

		    var product1 = new Product { Name = "Laptop", Price = 1500M };
		    var product2 = new Product { Name = "Laptop", Price = 2000M };
		    var product3 = new Product { Name = "Computer", Price = 2300M };

			_db.BeginTransaction();
			{
				Assert.AreEqual(1, _db.Products.Insert(product1, Conflict.Rollback));
				Assert.Throws<SQLiteException>(() => _db.Products.Insert(product2, Conflict.Rollback));
				Assert.AreEqual(1, _db.Products.Insert(product3, Conflict.Rollback));
			}
			Assert.Throws<SQLiteException>(() => _db.EndTransaction());
			
		    Assert.AreEqual(1, _db.Products.Count());
		    Assert.IsTrue(_db.Products.Any(p => p.Id == 1 && p.Name == "Computer" && p.Price == 2300M));
	    }


		[Test]
		[Category("Delete")]
	    public void TestDeleteAll()
	    {
			Assert.AreEqual(0, _db.Products.Count());

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
		    Assert.AreEqual(products.Length, _db.Products.Count());

		    int count = _db.Products.DeleteAll();
			Assert.AreEqual(products.Length, count);
			Assert.AreEqual(0, _db.Products.Count());
	    }

	    [Test]
	    [Category("Delete")]
	    public void TestDelete()
	    {
		    Assert.AreEqual(0, _db.Products.Count());

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

		    Assert.AreEqual(products.Length, count);
		    Assert.AreEqual(products.Length, _db.Products.Count());

		    _db.Products.Delete(p => p.Id == 2);

			Assert.AreEqual(products.Length - 1, _db.Products.Count());
		}

	    [Test]
	    [Category("Delete")]
	    public void TestDeleteMultiple()
	    {
		    Assert.AreEqual(0, _db.Products.Count());

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

		    Assert.AreEqual(products.Length, count);
		    Assert.AreEqual(products.Length, _db.Products.Count());

		    _db.Products.Delete(p => p.Price > 1000);

		    Assert.AreEqual(products.Length - 3, _db.Products.Count());
		}


	    [Test]
	    [Category("Update")]
	    public void TestUpdate()
	    {
		    Assert.AreEqual(0, _db.Products.Count());

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
		    Assert.AreEqual(products.Length, _db.Products.Count());

			Assert.IsTrue(_db.Products.Any(p => p.Id == 4 && p.Name == "Piano" && p.Price == 2500));

		    var product = _db.Products.Single(p => p.Id == 4);
		    product.Price = 1234;

		    _db.Products.Update(product);

		    Assert.IsTrue(_db.Products.Any(p => p.Id == 4 && p.Name == "Piano" && p.Price == 1234));
		}
	}
}
