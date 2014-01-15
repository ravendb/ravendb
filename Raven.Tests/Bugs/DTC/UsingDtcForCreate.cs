using System.Threading;
using System.Transactions;
using Raven.Client.Document;
using Xunit;
using Xunit.Extensions;

namespace Raven.Tests.Bugs.DTC
{
	public class UsingDtcForCreate : RemoteClientTest
	{
		[Theory]
		[InlineData(true)]
		[InlineData(false)]
		public void ShouldWork(bool runinmemory)
		{
			using (GetNewServer(runInMemory: runinmemory))
			using(var store = new DocumentStore
			{
				Url = "http://localhost:8079",
				Conventions =
					{
						ShouldCacheRequest = s => false
					}
			}.Initialize())
			{
				using (var tx = new TransactionScope())
				{
					using (var s = store.OpenSession())
					{
						s.Store(new Tester {Id = "tester123", Name = "Blah"});
						s.SaveChanges();
					}

					tx.Complete();
				}

				using (var s = store.OpenSession())
				{
					s.Store(new Tester {Id = "tester1234", Name = "Blah"});
					s.SaveChanges();
				}

				using (var s = store.OpenSession())
				{
					s.Advanced.AllowNonAuthoritativeInformation = false;
					Assert.NotNull(s.Load<Tester>("tester1234"));
					Assert.NotNull(s.Load<Tester>("tester123"));
				}
			}
		}

		public class Tester
		{
			public string Id { get; set; }
			public string Name { get; set; }
		}
	}
}