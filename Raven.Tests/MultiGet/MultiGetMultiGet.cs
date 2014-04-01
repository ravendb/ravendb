using Raven.Client.Document;
using Raven.Tests.Common;
using Raven.Tests.Linq;
using Xunit;
using System.Linq;

namespace Raven.Tests.MultiGet
{
	public class MultiGetMultiGet : RavenTest
	{
		[Fact]
		public void MultiGetShouldBehaveTheSameForLazyAndNotLazy()
		{
			using (GetNewServer())
			using (var store = new DocumentStore { Url = "http://localhost:8079" }.Initialize())
			{
				using (var session = store.OpenSession())
				{
					var result1 = session.Load<User>("users/1", "users/2");
					var result2 = session.Advanced.Lazily.Load<User>("users/3", "users/4");

					Assert.Equal(new User[2], result1);
					Assert.Equal(new User[2], result2.Value);
				}
			}
		}

		[Fact]
		public void UnlessAccessedLazyOperationsAreNoOp()
		{
			using(GetNewServer())
			using(var store = new DocumentStore{Url = "http://localhost:8079"}.Initialize())
			{
				using(var session = store.OpenSession())
				{
					var result1 = session.Advanced.Lazily.Load<User>("users/1", "users/2");
					var result2 = session.Advanced.Lazily.Load<User>("users/3", "users/4");
					Assert.Equal(0, session.Advanced.NumberOfRequests);
				}
			}
		}

		[Fact]
		public void LazyOperationsAreBatched()
		{
			using (GetNewServer())
			using (var store = new DocumentStore { Url = "http://localhost:8079" }.Initialize())
			{
				using (var session = store.OpenSession())
				{
					var result1 = session.Advanced.Lazily.Load<User>("users/1", "users/2");
					var result2 = session.Advanced.Lazily.Load<User>("users/3", "users/4");

					Assert.Equal(new User[2], result2.Value);
					Assert.Equal(1, session.Advanced.NumberOfRequests);

					Assert.Equal(new User[2], result1.Value);
					Assert.Equal(1, session.Advanced.NumberOfRequests);
				}

			}
		}

		[Fact]
		public void LazyMultiLoadOperationWouldBeInTheSession()
		{
			using (GetNewServer())
			using (var store = new DocumentStore { Url = "http://localhost:8079" }.Initialize())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new User());
					session.Store(new User());
					session.Store(new User());
					session.Store(new User());
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					var result1 = session.Advanced.Lazily.Load<User>("users/1", "users/2");
					var result2 = session.Advanced.Lazily.Load<User>("users/3", "users/4");
					var a = result2.Value;
					Assert.Equal(1, session.Advanced.NumberOfRequests);
					var b = result1.Value;
					Assert.Equal(1, session.Advanced.NumberOfRequests);

					foreach (var user in b.Concat(a))
					{
						Assert.NotNull(session.Advanced.GetMetadataFor(user));
					}
				}
			}
		}

		[Fact]
		public void LazyLoadOperationWillHandleIncludes()
		{
			using (GetNewServer())
			using (var store = new DocumentStore { Url = "http://localhost:8079" }.Initialize())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new User{Name = "users/2"});
					session.Store(new User());
					session.Store(new User{Name = "users/4"});
					session.Store(new User());
					session.SaveChanges();
				}
				using (var session = store.OpenSession())
				{
					var result1 = session.Advanced.Lazily
						.Include("Name")
						.Load<User>("users/1");
					var result2 = session.Advanced.Lazily
						.Include("Name")
						.Load<User>("users/3");

					Assert.NotNull(result1.Value);
					Assert.NotNull(result2.Value);
					Assert.Equal(1, session.Advanced.NumberOfRequests);
					Assert.Equal(1, session.Advanced.NumberOfRequests);

					Assert.True(session.Advanced.IsLoaded("users/2"));
					Assert.True(session.Advanced.IsLoaded("users/4"));
				}

			}
		}
	}
}