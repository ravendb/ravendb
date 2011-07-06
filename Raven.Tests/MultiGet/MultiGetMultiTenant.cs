using System.Linq;
using Raven.Client.Document;
using Raven.Tests.Bugs;
using Xunit;
using Raven.Client.Linq;
using Raven.Client.Extensions;

namespace Raven.Tests.MultiGet
{
	public class MultiGetMultiTenant : RemoteClientTest
	{
		[Fact]
		public void CanUseLazyWithMultiTenancy()
		{
			using (var server = GetNewServer())
			using (var store = new DocumentStore { Url = "http://localhost:8080" }.Initialize())
			{
				store.DatabaseCommands.EnsureDatabaseExists("test");

				using (var session = store.OpenSession("test"))
				{
					session.Store(new User { Name = "oren" });
					session.Store(new User { Name = "ayende" });
					session.Store(new User());
					session.SaveChanges();
				}
				using (var session = store.OpenSession("test"))
				{
					var result1 = session.Advanced.Lazily.Load<User>("users/1");
					var result2 = session.Advanced.Lazily.Load<User>("users/2");
					Assert.NotNull(result1.Value);
					Assert.NotNull(result2.Value);
				}
			}
		}
	}
}