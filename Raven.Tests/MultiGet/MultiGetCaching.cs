using System.Linq;
using Raven.Client.Linq;
using Raven.Client.Document;
using Raven.Tests.Linq;
using Xunit;

namespace Raven.Tests.MultiGet
{
	public class MultiGetCaching : RemoteClientTest
	{
		[Fact]
		public void CanCacheLazyQueryResults()
		{
			using (GetNewServer())
			using (var store = new DocumentStore { Url = "http://localhost:8080" }.Initialize())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new User { Name = "oren" });
					session.Store(new User());
					session.Store(new User { Name = "ayende" });
					session.Store(new User());
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					session.Query<User>()
						.Customize(x => x.WaitForNonStaleResults())
						.Where(x => x.Name == "test")
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
					Assert.Equal(0, store.JsonRequestFactory.NumberOfCachedRequests);
				}

				using (var session = store.OpenSession())
				{
					var result1 = session.Query<User>().Where(x => x.Name == "oren").Lazily();
					var result2 = session.Query<User>().Where(x => x.Name == "ayende").Lazily();
					Assert.NotEmpty(result2.Value);


					Assert.Equal(1, session.Advanced.NumberOfRequests);
					Assert.NotEmpty(result1.Value);
					Assert.Equal(1, session.Advanced.NumberOfRequests);

					Assert.Equal(2, store.JsonRequestFactory.NumberOfCachedRequests);
				}
			}
		}


		[Fact]
		public void CanCacheLazyQueryAndMultiLoadResults()
		{
			using (GetNewServer())
			using (var store = new DocumentStore { Url = "http://localhost:8080" }.Initialize())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new User { Name = "oren" });
					session.Store(new User());
					session.Store(new User { Name = "ayende" });
					session.Store(new User());
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					session.Query<User>()
						.Customize(x => x.WaitForNonStaleResults())
						.Where(x => x.Name == "test")
						.ToList();
				}
				using (var session = store.OpenSession())
				{
					var items = session.Advanced.Lazily.Load<User>("users/2", "users/4");
					var result1 = session.Query<User>().Where(x => x.Name == "oren").Lazily();
					var result2 = session.Query<User>().Where(x => x.Name == "ayende").Lazily();
					Assert.NotEmpty(result2.Value);
					Assert.NotEmpty(items.Value);
					Assert.Equal(1, session.Advanced.NumberOfRequests);
					Assert.NotEmpty(result1.Value);
					Assert.Equal(1, session.Advanced.NumberOfRequests);
					Assert.Equal(0, store.JsonRequestFactory.NumberOfCachedRequests);
				}

				using (var session = store.OpenSession())
				{
					var items = session.Advanced.Lazily.Load<User>("users/2", "users/4"); 
					var result1 = session.Query<User>().Where(x => x.Name == "oren").Lazily();
					var result2 = session.Query<User>().Where(x => x.Name == "ayende").Lazily();
					Assert.NotEmpty(result2.Value);
					Assert.NotEmpty(items.Value);
					Assert.Equal(1, session.Advanced.NumberOfRequests);
					Assert.NotEmpty(result1.Value);
					Assert.Equal(1, session.Advanced.NumberOfRequests);

					Assert.Equal(3, store.JsonRequestFactory.NumberOfCachedRequests);
				}
			}
		}


		[Fact]
		public void CanMixCachingForBatchAndNonBatched_BatchedFirst()
		{
			using (GetNewServer())
			using (var store = new DocumentStore { Url = "http://localhost:8080" }.Initialize())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new User { Name = "oren" });
					session.Store(new User());
					session.Store(new User { Name = "ayende" });
					session.Store(new User());
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					session.Query<User>()
						.Customize(x => x.WaitForNonStaleResults())
						.Where(x => x.Name == "test")
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

				using (var session = store.OpenSession())
				{
					session.Query<User>().Where(x => x.Name == "oren").ToArray();
					session.Query<User>().Where(x => x.Name == "ayende").ToArray();

					Assert.Equal(2, store.JsonRequestFactory.NumberOfCachedRequests);
				}
			}
		}

		[Fact]
		public void CanMixCachingForBatchAndNonBatched_IndividualFirst()
		{
			using (GetNewServer())
			using (var store = new DocumentStore { Url = "http://localhost:8080" }.Initialize())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new User { Name = "oren" });
					session.Store(new User());
					session.Store(new User { Name = "ayende" });
					session.Store(new User());
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					session.Query<User>()
						.Customize(x => x.WaitForNonStaleResults())
						.Where(x => x.Name == "test")
						.ToList();
				}
				

				using (var session = store.OpenSession())
				{
					session.Query<User>().Where(x => x.Name == "oren").ToArray();
					session.Query<User>().Where(x => x.Name == "ayende").ToArray();
				}

				using (var session = store.OpenSession())
				{
					var result1 = session.Query<User>().Where(x => x.Name == "oren").Lazily();
					var result2 = session.Query<User>().Where(x => x.Name == "ayende").Lazily();
					Assert.NotEmpty(result2.Value);

					Assert.Equal(1, session.Advanced.NumberOfRequests);
					Assert.NotEmpty(result1.Value);
					Assert.Equal(1, session.Advanced.NumberOfRequests);
					Assert.Equal(2, store.JsonRequestFactory.NumberOfCachedRequests);
				}
			}
		}
	}
}