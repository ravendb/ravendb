using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestPlatform.UnitTestFramework;
using Raven.Abstractions.Indexing;
using Raven.Client.Extensions;
using Raven.Client;
using Raven.Tests.Document;
using Raven.Tests.WinRT.Entities;

namespace Raven.Tests.WinRT
{
	[TestClass]
	public class AsyncLinqQueryTests : RavenTestBase
	{
		[TestMethod]
		[Ignore] // ToList should be an Obsolete method which will throw NotSupportedException exception when invoking.
		// [ExpectedException(typeof(NotSupportedException))]
		public async Task CallingToListRaisesAnException()
		{
			var dbname = GenerateNewDatabaseName("AsyncLinqQueryTests.CallingToListRaisesAnException");
			using (var store = NewDocumentStore())
			{
				await store.AsyncDatabaseCommands.Admin.EnsureDatabaseExistsAsync(dbname);

				using (var session = store.OpenAsyncSession(dbname))
				{
					//NOTE: shouldn't compile
					//var query = session.Query<Company>()
					//            .Where(x => x.Name == "Doesn't Really Matter")
					//            .ToList();

					// should compile
					var list = new List<string>().ToList();
				}
			}
		}

		[TestMethod]
		public async Task CanPerformASimpleWhere()
		{
			var dbname = GenerateNewDatabaseName("AsyncLinqQueryTests.CanPerformASimpleWhere");
			using (var store = NewDocumentStore())
			{
				await store.AsyncDatabaseCommands.Admin.EnsureDatabaseExistsAsync(dbname);

				var entity = new Company {Name = "Async Company #1", Id = "companies/1"};
				using (var session = store.OpenAsyncSession(dbname))
				{
					await session.StoreAsync(entity);
					await session.SaveChangesAsync();
				}

				using (var session = store.OpenAsyncSession(dbname))
				{
					var companies = await session.Query<Company>()
					                   .Where(x => x.Name == "Async Company #1")
					                   .ToListAsync();

					Assert.AreEqual(1, companies.Count);
					Assert.AreEqual("Async Company #1", companies[0].Name);
				}
			}
		}

		[TestMethod]
		public async Task CanGetTotalCount()
		{
			var dbname = GenerateNewDatabaseName("AsyncLinqQueryTests.CanGetTotalCount");
			using (var store = NewDocumentStore())
			{
				await store.AsyncDatabaseCommands.Admin.EnsureDatabaseExistsAsync(dbname);

				var entity = new Company {Name = "Async Company #1", Id = "companies/1"};
				using (var session = store.OpenAsyncSession(dbname))
				{
					await session.StoreAsync(entity);
					await session.SaveChangesAsync();
				}

				using (var session = store.OpenAsyncSession(dbname))
				{
					RavenQueryStatistics stats;
					var count = await session.Query<Company>()
					                         .Customize(x => x.WaitForNonStaleResults())
					                         .Statistics(out stats)
					                         .Where(x => x.Name == "Async Company #1")
					                         .CountAsync();

					Assert.AreEqual(1, count);
				}
			}
		}

		[TestMethod]
		public async Task CanUseAny()
		{
			var dbname = GenerateNewDatabaseName("AsyncLinqQueryTests.CanUseAny");
			using (var store = NewDocumentStore())
			{
				await store.AsyncDatabaseCommands.Admin.EnsureDatabaseExistsAsync(dbname);

				var entity = new Company { Name = "Async Company #1", Id = "companies/1" };
				using (var session = store.OpenAsyncSession(dbname))
				{
					await session.StoreAsync(entity);
					await session.SaveChangesAsync();
				}

				using (var session = store.OpenAsyncSession(dbname))
				{
					RavenQueryStatistics stats;
					var condition = await session.Query<Company>()
											 .Customize(x => x.WaitForNonStaleResults())
											 .Statistics(out stats)
											 .Where(x => x.Name == "Async Company #1")
											 .AnyAsync();

					Assert.IsTrue(condition);
				}
			}
		}

		[TestMethod]
		public async Task CanGetTotalCountFromStats()
		{
			var dbname = GenerateNewDatabaseName("AsyncLinqQueryTests.CanGetTotalCountFromStats");
			using (var store = NewDocumentStore())
			{
				await store.AsyncDatabaseCommands.Admin.EnsureDatabaseExistsAsync(dbname);

				var entity = new Company {Name = "Async Company #1", Id = "companies/1"};
				using (var session = store.OpenAsyncSession(dbname))
				{
					await session.StoreAsync(entity);
					await session.SaveChangesAsync();
				}

				using (var session = store.OpenAsyncSession(dbname))
				{
					RavenQueryStatistics stats;
					var companies = await session.Query<Company>()
						.Statistics(out stats)
						.Where(x => x.Name == "Async Company #1")
						.ToListAsync();

					Assert.AreEqual(1, stats.TotalResults);
				}
			}
		}

		[TestMethod]
		public async Task CanQuerySpecificIndex()
		{
			var dbname = GenerateNewDatabaseName("AsyncLinqQueryTests.CanQuerySpecificIndex");
			using (var store = NewDocumentStore())
			{
				await store.AsyncDatabaseCommands.Admin.EnsureDatabaseExistsAsync(dbname);

				await store.AsyncDatabaseCommands.ForDatabase(dbname).PutIndexAsync("test", new IndexDefinition
				{
					Map = "from c in docs select new { c.Name }"
				}, true);

				var entity = new Company {Name = "Async Company #1", Id = "companies/1"};
				using (var session = store.OpenAsyncSession(dbname))
				{
					await session.StoreAsync(entity);
					await session.SaveChangesAsync();
				}

				using (var session = store.OpenAsyncSession(dbname))
				{
					RavenQueryStatistics stats;
					var companies = await session.Query<Company>("test")
					                         .Customize(x => x.WaitForNonStaleResults())
					                         .Statistics(out stats)
					                         .Where(x => x.Name == "Async Company #1")
					                         .ToListAsync();

					Assert.IsFalse(companies.Count == 0);
					Assert.AreEqual(1, stats.TotalResults);
				}
			}
		}


		[TestMethod]
		public async Task CanTestTwoConditionsInAWhereClause()
		{
			var dbname = GenerateNewDatabaseName("AsyncLinqQueryTests.CanTestTwoConditionsInAWhereClause");
			using (var store = NewDocumentStore())
			{
				await store.AsyncDatabaseCommands.Admin.EnsureDatabaseExistsAsync(dbname);

				using (var session = store.OpenAsyncSession(dbname))
				{
					await session.StoreAsync(new Company {Name = "Async Company", Phone = 55555, Id = "companies/1"});
					await session.StoreAsync(new Company {Name = "Async Company", Phone = 12345, Id = "companies/2"});
					await session.SaveChangesAsync();
				}

				using (var session = store.OpenAsyncSession(dbname))
				{
					var companies = await session.Query<Company>()
					                             .Where(x => x.Name == "Async Company" && x.Phone == 12345)
					                             .ToListAsync();

					Assert.AreEqual(1, companies.Count);
					Assert.AreEqual(12345, companies[0].Phone);
				}
			}
		}

		[TestMethod]
		public async Task CanQueryOnNotEqual()
		{
			var dbname = GenerateNewDatabaseName("AsyncLinqQueryTests.CanQueryOnNotEqual");
			using (var store = NewDocumentStore())
			{
				await store.AsyncDatabaseCommands.Admin.EnsureDatabaseExistsAsync(dbname);

				using (var session = store.OpenAsyncSession(dbname))
				{
					await session.StoreAsync(new Company {Name = "Ayende"});
					await session.StoreAsync(new Company { Name = "Oren" });
					await session.SaveChangesAsync();
				}

				using (var session = store.OpenAsyncSession(dbname))
				{
					var companies = await session.Query<Company>()
					                             .Where(x => x.Name != "Oren")
					                             .ToListAsync();

					Assert.AreEqual(1, companies.Count);
				}
			}
		}

		[TestMethod]
		public async Task CanPerformAnOrderBy()
		{
			var dbname = GenerateNewDatabaseName("AsyncLinqQueryTests.CanPerformAnOrderBy");
			using (var store = NewDocumentStore())
			{
				await store.AsyncDatabaseCommands.Admin.EnsureDatabaseExistsAsync(dbname);

				using (var session = store.OpenAsyncSession(dbname))
				{
					await session.StoreAsync(new Company {Name = "Moon Dog", Id = "companies/1"});
					await session.StoreAsync(new Company {Name = "Alpha Dog", Id = "companies/2"});
					await session.StoreAsync(new Company {Name = "Loony Lin", Id = "companies/3"});
					await session.StoreAsync(new Company {Name = "Betty Boop", Id = "companies/4"});
					await session.SaveChangesAsync();
				}

				using (var session = store.OpenAsyncSession(dbname))
				{
					var companies = await session.Query<Company>()
					                             .OrderBy(x => x.Name)
					                             .ToListAsync();

					Assert.AreEqual(4, companies.Count);
					Assert.AreEqual("Alpha Dog", companies[0].Name);
					Assert.AreEqual("Betty Boop", companies[1].Name);
					Assert.AreEqual("Loony Lin", companies[2].Name);
				}
			}
		}

		[TestMethod]
		public async Task CanPerformAWhereStartsWith()
		{
			var dbname = GenerateNewDatabaseName("AsyncLinqQueryTests.CanPerformAWhereStartsWith");
			using (var store = NewDocumentStore())
			{
				await store.AsyncDatabaseCommands.Admin.EnsureDatabaseExistsAsync(dbname);

				using (var session = store.OpenAsyncSession(dbname))
				{
					await session.StoreAsync(new Company {Name = "Async Company #1", Id = "companies/1"});
					await session.StoreAsync(new Company {Name = "Async Company #2", Id = "companies/2"});
					await session.StoreAsync(new Company {Name = "Different Company", Id = "companies/3"});
					await session.SaveChangesAsync();
				}

				using (var session = store.OpenAsyncSession(dbname))
				{
					var companies = await session.Query<Company>()
					                             .Where(x => x.Name.StartsWith("Async"))
					                             .ToListAsync();

					Assert.AreEqual(2, companies.Count);
				}
			}
		}

		[TestMethod]
		public async Task CanPerformAnIncludeInALinqQuery()
		{
			var dbname = GenerateNewDatabaseName("AsyncLinqQueryTests.CanPerformAnIncludeInALinqQuery");
			using (var store = NewDocumentStore())
			{
				store.Conventions.AllowQueriesOnId = true;
				await store.AsyncDatabaseCommands.Admin.EnsureDatabaseExistsAsync(dbname);

				var customer = new Customer {Name = "Customer #1", Id = "customer/1", Email = "someone@customer.com"};
				var order = new Order {Id = "orders/1", Note = "Hello", Customer = new DenormalizedReference {Id = customer.Id, Name = customer.Name}};
				using (var session = store.OpenAsyncSession(dbname))
				{
					await session.StoreAsync(customer);
					await session.StoreAsync(order);
					await session.SaveChangesAsync();
				}

				using (var session = store.OpenAsyncSession(dbname))
				{
					var orders = await session.Query<Order>()
					                          .Include(x => x.Customer.Id)
					                          .Where(x => x.Id == "orders/1")
					                          .ToListAsync();

					Assert.AreEqual("Hello", orders[0].Note);

					// NOTE: this call should not hit the server 
					var load = await session.LoadAsync<Customer>(customer.Id);

					Assert.AreEqual(1, session.Advanced.NumberOfRequests);
				}
			}
		}

		[TestMethod]
		[Ignore]
		public async Task CanPerformAProjectionInALinqQuery()
		{
			var dbname = GenerateNewDatabaseName("AsyncLinqQueryTests.CanPerformAProjectionInALinqQuery");
			using (var store = NewDocumentStore())
			{
				await store.AsyncDatabaseCommands.Admin.EnsureDatabaseExistsAsync(dbname);

				var entity = new Company {Name = "Async Company #1", Id = "companies/1"};
				using (var session = store.OpenAsyncSession(dbname))
				{
					await session.StoreAsync(entity);
					await session.SaveChangesAsync();
				}

				using (var session = store.OpenAsyncSession(dbname))
				{
					var companies = await session.Query<Company>()
					                         .Where(x => x.Name == "Async Company #1")
					                         .AsProjection<TheCompanyName>()
					                         .ToListAsync();

					//NOTE: it seems that the fields from the projection are not propagated to the query,
					//		 this manifests as a problem casting the type, because (since it does see the projected fields)
					//		 it assumes that the must be the original entity (i.e., Company)

					Assert.AreEqual(1, companies.Count);
					Assert.AreEqual("Async Company #1", companies[0].Name);
				}
			}
		}

		[TestMethod]
		public async Task CanPerformAnAny()
		{
			var dbname = GenerateNewDatabaseName("AsyncLinqQueryTests.CanPerformAnAny");
			using (var store = NewDocumentStore())
			{
				await store.AsyncDatabaseCommands.Admin.EnsureDatabaseExistsAsync(dbname);

				using (var session = store.OpenAsyncSession(dbname))
				{
					await session.StoreAsync(new Order
					{
						Id = "orders/1",
						Lines = new List<OrderLine> {new OrderLine {Quantity = 1}, new OrderLine {Quantity = 2}}
					});
					await session.StoreAsync(new Order
					{
						Id = "orders/2",
						Lines = new List<OrderLine> {new OrderLine {Quantity = 1}, new OrderLine {Quantity = 2}}
					});
					await session.StoreAsync(new Order
					{
						Id = "orders/3",
						Lines = new List<OrderLine> {new OrderLine {Quantity = 1}, new OrderLine {Quantity = 1}}

					});

					await session.SaveChangesAsync();
				}

				using (var session = store.OpenAsyncSession(dbname))
				{
					var orders = await session.Query<Order>()
					                          .Customize(x => x.WaitForNonStaleResults())
					                          .Where(x => x.Lines.Any(line => line.Quantity > 1))
					                          .ToListAsync();

					Assert.AreEqual(2, orders.Count);
				}
			}
		}
	}
}