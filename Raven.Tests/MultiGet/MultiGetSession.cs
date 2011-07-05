using System;
using Raven.Client.Document;
using Raven.Tests.Linq;
using Xunit;

namespace Raven.Tests.MultiGet
{
	public class MultiGetSession : RemoteClientTest
	{
		[Fact]
		public void UnlessAccessedLazyOpertionsAreNoOp()
		{
			using(GetNewServer())
			using(var store = new DocumentStore{Url = "http://localhost:8080"}.Initialize())
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
			using (var store = new DocumentStore { Url = "http://localhost:8080" }.Initialize())
			{
				using (var session = store.OpenSession())
				{
					var result1 = session.Advanced.Lazily.Load<User>("users/1", "users/2");
					var result2 = session.Advanced.Lazily.Load<User>("users/3", "users/4");
					GC.KeepAlive(result2.Value);
					var before = session.Advanced.NumberOfRequests;
					GC.KeepAlive(result1.Value);
					Assert.Equal(before, session.Advanced.NumberOfRequests);
				}

			}
		}
	}
}