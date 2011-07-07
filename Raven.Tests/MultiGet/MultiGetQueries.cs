using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Document;
using Raven.Tests.Linq;
using Xunit;
using Raven.Client.Linq;

namespace Raven.Tests.MultiGet
{
	public class MultiGetQueries : RemoteClientTest
	{
		[Fact]
		public void UnlessAccessedLazyQueriesAreNoOp()
		{
			using (GetNewServer())
			using (var store = new DocumentStore { Url = "http://localhost:8080" }.Initialize())
			{
				using (var session = store.OpenSession())
				{
					var result1 = session.Query<User>().Where(x => x.Name == "oren").Lazily();
					var result2 = session.Query<User>().Where(x => x.Name == "ayende").Lazily();
					Assert.Equal(0, session.Advanced.NumberOfRequests);
				}

			}
		}

		[Fact]
		public void LazyOperationsAreBatched()
		{
			using (GetNewServer())
			using (var store = new DocumentStore { Url = "http://localhost:8080" }.Initialize())
			{
				using (var session = store.OpenSession())
				{
					var result1 = session.Query<User>().Where(x => x.Name == "oren").Lazily();
					var result2 = session.Query<User>().Where(x => x.Name == "ayende").Lazily();
					Assert.Empty(result2.Value);
					Assert.Equal(1, session.Advanced.NumberOfRequests);
					Assert.Empty(result1.Value);
					Assert.Equal(1, session.Advanced.NumberOfRequests);
				}

			}
		}

		[Fact]
		public void LazyMultiLoadOperationWouldBeInTheSession()
		{
			using (GetNewServer())
			using (var store = new DocumentStore { Url = "http://localhost:8080" }.Initialize())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new User{Name = "oren"});
					session.Store(new User());
					session.Store(new User{Name = "ayende"});
					session.Store(new User());
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					session.Query<User>()
						.Customize(x=>x.WaitForNonStaleResults())
						.Where(x => x.Name == "oren")
						.ToList();
				}
				using (var session = store.OpenSession())
				{
					var result1 = session.Query<User>().Where(x => x.Name == "oren").Lazily();
					var result2 = session.Query<User>().Where(x => x.Name == "ayende").Lazily();
					Assert.NotEmpty(result2.Value);

					Assert.Equal(1, session.Advanced.NumberOfRequests);
					Assert.NotEmpty(result1.Value);
					Assert.Equal(1, session.Advanced.NumberOfRequests);
					
				}

			}
		}

		[Fact]
		public void LazyWithProjection()
		{
			using (GetNewServer())
			using (var store = new DocumentStore { Url = "http://localhost:8080" }.Initialize())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new User { Name = "oren" });
					session.Store(new User { Name = "ayende" });
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					session.Query<User>()
						.Customize(x => x.WaitForNonStaleResults())
						.Where(x => x.Name == "oren")
						.ToList();
				}
				using (var session = store.OpenSession())
				{
					var result1 = session.Query<User>().Where(x => x.Name == "oren")
						.Select(x=> new { x.Name })
						.Lazily();

					Assert.Equal("oren", result1.Value.First().Name);
				}

			}
		}


		[Fact]
		public void LazyWithProjection2()
		{
			using (GetNewServer())
			using (var store = new DocumentStore { Url = "http://localhost:8080" }.Initialize())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new User { Name = "oren" });
					session.Store(new User { Name = "ayende" });
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					session.Query<User>()
						.Customize(x => x.WaitForNonStaleResults())
						.Where(x => x.Name == "oren")
						.ToList();
				}
				using (var session = store.OpenSession())
				{
					var result1 = session.Query<User>().Where(x => x.Name == "oren")
						.Select(x => new { x.Name })
						.ToArray();

					Assert.Equal("oren", result1.First().Name);
				}

			}
		}

		[Fact]
		public void LazyMultiLoadOperationWouldBeInTheSession_WithNonStaleResponse()
		{
			using (GetNewServer())
			using (var store = new DocumentStore { Url = "http://localhost:8080" }.Initialize())
			{
				using (var session = store.OpenSession())
				{
					session.Query<User>().ToArray();

					session.Store(new User { Name = "oren" });
					session.Store(new User());
					session.Store(new User { Name = "ayende" });
					session.Store(new User());
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					var result1 = session.Query<User>().Customize(x=>x.WaitForNonStaleResults(TimeSpan.FromMinutes(5))).Where(x => x.Name == "oren").Lazily();
					var result2 = session.Query<User>().Customize(x => x.WaitForNonStaleResults(TimeSpan.FromMinutes(5))).Where(x => x.Name == "ayende").Lazily();
					Assert.NotEmpty(result2.Value);

					Assert.Equal(1, session.Advanced.NumberOfRequests);
					Assert.NotEmpty(result1.Value);
					Assert.Equal(1, session.Advanced.NumberOfRequests);

				}

			}
		}
	}
}