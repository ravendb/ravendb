using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Silverlight.Testing;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Raven.Abstractions.Indexing;
using Raven.Client.Document;
using Raven.Client.Extensions;
using Raven.Client.Linq;
using Raven.Tests.Document;
using Raven.Tests.Silverlight.Entities;

namespace Raven.Tests.Silverlight
{
	public class AsyncLinqQueryTests : RavenTestBase
	{
		[Asynchronous]
		[Ignore] // ToList should be an Obsolete method which will throw NotSupportedException exception when invoking.
		// [ExpectedException(typeof(NotSupportedException))]
		public IEnumerable<Task> CallingToListRaisesAnException()
		{
			var dbname = GenerateNewDatabaseName();
			using (var documentStore = new DocumentStore {Url = Url + Port}.Initialize())
			{
				yield return documentStore.AsyncDatabaseCommands.EnsureDatabaseExistsAsync(dbname);

				using (var session = documentStore.OpenAsyncSession(dbname))
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

		[Asynchronous]
		public IEnumerable<Task> CanPerformASimpleWhere()
		{
			var dbname = GenerateNewDatabaseName();
			using (var documentStore = new DocumentStore {Url = Url + Port}.Initialize())
			{
				yield return documentStore.AsyncDatabaseCommands.EnsureDatabaseExistsAsync(dbname);

				var entity = new Company {Name = "Async Company #1", Id = "companies/1"};
				using (var session = documentStore.OpenAsyncSession(dbname))
				{
					session.Store(entity);
					yield return session.SaveChangesAsync();
				}

				using (var session = documentStore.OpenAsyncSession(dbname))
				{
					var query = session.Query<Company>()
						.Where(x => x.Name == "Async Company #1")
						.ToListAsync();
					yield return query;

					Assert.AreEqual(1, query.Result.Count);
					Assert.AreEqual("Async Company #1", query.Result[0].Name);
				}
			}
		}

		[Asynchronous]
		public IEnumerable<Task> CanGetTotalCount()
		{
			var dbname = GenerateNewDatabaseName();
			using (var documentStore = new DocumentStore {Url = Url + Port}.Initialize())
			{
				yield return documentStore.AsyncDatabaseCommands.EnsureDatabaseExistsAsync(dbname);

				var entity = new Company {Name = "Async Company #1", Id = "companies/1"};
				using (var session = documentStore.OpenAsyncSession(dbname))
				{
					session.Store(entity);
					yield return session.SaveChangesAsync();
				}

				using (var session = documentStore.OpenAsyncSession(dbname))
				{
					RavenQueryStatistics stats;
					var query = session.Query<Company>()
						.Customize(x => x.WaitForNonStaleResults())
						.Statistics(out stats)
						.Where(x => x.Name == "Async Company #1")
						.CountAsync();
					yield return query;

					Assert.AreEqual(1, query.Result);
				}
			}
		}

		[Asynchronous]
		public IEnumerable<Task> CanGetTotalCountFromStats()
		{
			var dbname = GenerateNewDatabaseName();
			using (var documentStore = new DocumentStore {Url = Url + Port}.Initialize())
			{
				yield return documentStore.AsyncDatabaseCommands.EnsureDatabaseExistsAsync(dbname);

				var entity = new Company {Name = "Async Company #1", Id = "companies/1"};
				using (var session = documentStore.OpenAsyncSession(dbname))
				{
					session.Store(entity);
					yield return session.SaveChangesAsync();
				}

				using (var session = documentStore.OpenAsyncSession(dbname))
				{
					RavenQueryStatistics stats;
					var query = session.Query<Company>()
						.Statistics(out stats)
						.Where(x => x.Name == "Async Company #1")
						.ToListAsync();
					yield return query;

					Assert.AreEqual(1, stats.TotalResults);
				}
			}
		}

		[Asynchronous]
		public IEnumerable<Task> CanQuerySpecificIndex()
		{
			var dbname = GenerateNewDatabaseName();
			using (var documentStore = new DocumentStore {Url = Url + Port}.Initialize())
			{
				yield return documentStore.AsyncDatabaseCommands.EnsureDatabaseExistsAsync(dbname);

				yield return documentStore.AsyncDatabaseCommands.ForDatabase(dbname).PutIndexAsync("test", new IndexDefinition
				                                                                                           	{
				                                                                                           		Map = "from c in docs select new { c.Name }"
				                                                                                           	}, true);

				var entity = new Company {Name = "Async Company #1", Id = "companies/1"};
				using (var session = documentStore.OpenAsyncSession(dbname))
				{
					session.Store(entity);
					yield return session.SaveChangesAsync();
				}

				using (var session = documentStore.OpenAsyncSession(dbname))
				{
					RavenQueryStatistics stats;
					var query = session.Query<Company>("test")
						.Customize(x => x.WaitForNonStaleResults())
						.Statistics(out stats)
						.Where(x => x.Name == "Async Company #1")
						.ToListAsync();
					yield return query;

					Assert.IsFalse(query.Result.Count == 0);

					Assert.AreEqual(1, stats.TotalResults);
				}
			}
		}


		[Asynchronous]
		public IEnumerable<Task> CanTestTwoConditionsInAWhereClause()
		{
			var dbname = GenerateNewDatabaseName();
			using (var documentStore = new DocumentStore {Url = Url + Port}.Initialize())
			{
				yield return documentStore.AsyncDatabaseCommands.EnsureDatabaseExistsAsync(dbname);

				using (var session = documentStore.OpenAsyncSession(dbname))
				{
					session.Store(new Company {Name = "Async Company", Phone = 55555, Id = "companies/1"});
					session.Store(new Company {Name = "Async Company", Phone = 12345, Id = "companies/2"});
					yield return session.SaveChangesAsync();
				}

				using (var session = documentStore.OpenAsyncSession(dbname))
				{
					var query = session.Query<Company>()
						.Where(x => x.Name == "Async Company" && x.Phone == 12345)
						.ToListAsync();
					yield return query;

					Assert.AreEqual(1, query.Result.Count);
					Assert.AreEqual(12345, query.Result[0].Phone);
				}
			}
		}

		[Asynchronous]
		public IEnumerable<Task> CanQueryOnNotEqual()
		{
			var dbname = GenerateNewDatabaseName();
			using (var documentStore = new DocumentStore {Url = Url + Port}.Initialize())
			{
				yield return documentStore.AsyncDatabaseCommands.EnsureDatabaseExistsAsync(dbname);

				using (var s = documentStore.OpenAsyncSession(dbname))
				{
					s.Store(new Company {Name = "Ayende"});
					s.Store(new Company {Name = "Oren"});
					yield return s.SaveChangesAsync();
				}

				using (var s = documentStore.OpenAsyncSession(dbname))
				{
					var query = s.Query<Company>()
						.Where(x => x.Name != "Oren")
						.ToListAsync();
					yield return query;

					Assert.AreEqual(1, query.Result.Count);
				}
			}
		}

		[Asynchronous]
		public IEnumerable<Task> CanPerformAnOrderBy()
		{
			var dbname = GenerateNewDatabaseName();
			using (var documentStore = new DocumentStore {Url = Url + Port}.Initialize())
			{
				yield return documentStore.AsyncDatabaseCommands.EnsureDatabaseExistsAsync(dbname);

				using (var session = documentStore.OpenAsyncSession(dbname))
				{
					session.Store(new Company {Name = "Moon Dog", Id = "companies/1"});
					session.Store(new Company {Name = "Alpha Dog", Id = "companies/2"});
					session.Store(new Company {Name = "Loony Lin", Id = "companies/3"});
					session.Store(new Company {Name = "Betty Boop", Id = "companies/4"});
					yield return session.SaveChangesAsync();
				}

				using (var session = documentStore.OpenAsyncSession(dbname))
				{
					var query = session.Query<Company>()
						.OrderBy(x => x.Name)
						.ToListAsync();
					yield return query;

					Assert.AreEqual(4, query.Result.Count);
					Assert.AreEqual("Alpha Dog", query.Result[0].Name);
					Assert.AreEqual("Betty Boop", query.Result[1].Name);
					Assert.AreEqual("Loony Lin", query.Result[2].Name);
				}
			}
		}

		[Asynchronous]
		public IEnumerable<Task> CanPerformAWhereStartsWith()
		{
			var dbname = GenerateNewDatabaseName();
			using (var documentStore = new DocumentStore {Url = Url + Port}.Initialize())
			{
				yield return documentStore.AsyncDatabaseCommands.EnsureDatabaseExistsAsync(dbname);

				using (var session = documentStore.OpenAsyncSession(dbname))
				{
					session.Store(new Company {Name = "Async Company #1", Id = "companies/1"});
					session.Store(new Company {Name = "Async Company #2", Id = "companies/2"});
					session.Store(new Company {Name = "Different Company", Id = "companies/3"});
					yield return session.SaveChangesAsync();
				}

				using (var session = documentStore.OpenAsyncSession(dbname))
				{
					var query = session.Query<Company>()
						.Where(x => x.Name.StartsWith("Async"))
						.ToListAsync();
					yield return query;

					Assert.AreEqual(2, query.Result.Count);
				}
			}
		}

		[Asynchronous]
		public IEnumerable<Task> CanPerformAnIncludeInALinqQuery()
		{
			var dbname = GenerateNewDatabaseName();
			using (var documentStore = new DocumentStore {Url = Url + Port}.Initialize())
			{
				documentStore.Conventions.AllowQueriesOnId = true;
				yield return documentStore.AsyncDatabaseCommands.EnsureDatabaseExistsAsync(dbname);

				var customer = new Customer {Name = "Customer #1", Id = "customer/1", Email = "someone@customer.com"};
				var order = new Order {Id = "orders/1", Note = "Hello", Customer = new DenormalizedReference {Id = customer.Id, Name = customer.Name}};
				using (var session = documentStore.OpenAsyncSession(dbname))
				{
					session.Store(customer);
					session.Store(order);
					yield return session.SaveChangesAsync();
				}

				using (var session = documentStore.OpenAsyncSession(dbname))
				{
					var query = session.Query<Order>()
						.Include(x => x.Customer.Id)
						.Where(x => x.Id == "orders/1")
						.ToListAsync();
					yield return query;

					Assert.AreEqual("Hello", query.Result[0].Note);

					// NOTE: this call should not hit the server 
					var load = session.LoadAsync<Customer>(customer.Id);
					yield return load;

					Assert.AreEqual(1, session.Advanced.NumberOfRequests);
				}
			}
		}

		[Asynchronous]
		[Ignore]
		public IEnumerable<Task> CanPerformAProjectionInALinqQuery()
		{
			var dbname = GenerateNewDatabaseName();
			using (var documentStore = new DocumentStore {Url = Url + Port}.Initialize())
			{
				yield return documentStore.AsyncDatabaseCommands.EnsureDatabaseExistsAsync(dbname);

				var entity = new Company {Name = "Async Company #1", Id = "companies/1"};
				using (var session = documentStore.OpenAsyncSession(dbname))
				{
					session.Store(entity);
					yield return session.SaveChangesAsync();
				}

				using (var session = documentStore.OpenAsyncSession(dbname))
				{
					var query = session.Query<Company>()
						.Where(x => x.Name == "Async Company #1")
						.AsProjection<TheCompanyName>()
						.ToListAsync();
					yield return query;

					//NOTE: it seems that the fields from the projection are not proprogated to the query,
					//		 this manifests as a problem casting the type, because (since it does see the projected fields)
					//		 it assumes that the must be the original entity (i.e., Company)

					Assert.AreEqual(1, query.Result.Count);
					Assert.AreEqual("Async Company #1", query.Result[0].Name);
				}
			}
		}

		[Asynchronous]
		public IEnumerable<Task> CanPerformAnAny()
		{
			var dbname = GenerateNewDatabaseName();
			using (var documentStore = new DocumentStore {Url = Url + Port}.Initialize())
			{
				yield return documentStore.AsyncDatabaseCommands.EnsureDatabaseExistsAsync(dbname);

				using (var session = documentStore.OpenAsyncSession(dbname))
				{
					session.Store(new Order
					              	{
					              		Id = "orders/1",
					              		Lines = new List<OrderLine> {new OrderLine {Quantity = 1}, new OrderLine {Quantity = 2}}
					              	});
					session.Store(new Order
					              	{
					              		Id = "orders/2",
					              		Lines = new List<OrderLine> {new OrderLine {Quantity = 1}, new OrderLine {Quantity = 2}}
					              	});
					session.Store(new Order
					              	{
					              		Id = "orders/3",
					              		Lines = new List<OrderLine> {new OrderLine {Quantity = 1}, new OrderLine {Quantity = 1}}

					              	});

					yield return session.SaveChangesAsync();
				}

				using (var session = documentStore.OpenAsyncSession(dbname))
				{
					var query = session.Query<Order>()
						.Customize(x => x.WaitForNonStaleResults())
						.Where(x => x.Lines.Any(line => line.Quantity > 1))
						.ToListAsync();
					yield return query;

					Assert.AreEqual(2, query.Result.Count);
				}
			}
		}
	}
}