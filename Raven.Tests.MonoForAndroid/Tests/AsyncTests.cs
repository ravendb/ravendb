using System.Linq;
using System.Threading.Tasks;
using NUnit.Framework;
using Raven.Client.Linq;
using Raven.Client;

namespace Raven.Tests.MonoForAndroid.Tests
{
	public class AsyncTests : MonoForAndroidTestBase
	{
		[Test]
		public async Task CanLoadFromServerAsync()
		{
			using (var store = CreateDocumentStore())
			{
				using (var session = store.OpenAsyncSession())
				{
					await session.StoreAsync(new Info(), "infos/1234");
					await session.SaveChangesAsync();
				}

				using (var session = store.OpenAsyncSession())
				{
					var info = await session.LoadAsync<Info>(1234);
					Assert.NotNull(info);
				}
			}
		}

		[Test]
		public async Task CanQueryAsync()
		{
			using (var store = CreateDocumentStore())
			{
				using (var session = store.OpenAsyncSession())
				{
					await session.StoreAsync(new Info { Data = "First" });
					await session.StoreAsync(new Info { Data = "Other" });
					await session.SaveChangesAsync();
				}

				using (var session = store.OpenAsyncSession())
				{
					var results = await session.Query<Info>()
										 .Customize(x => x.WaitForNonStaleResultsAsOfNow())
										 .Where(x => x.Data == "First").ToListAsync();
					Assert.IsNotEmpty(results);
				}
			}
		}

		[Test]
		public async Task CanWriteToServerAsync()
		{
			using (var store = CreateDocumentStore())
			using (var session = store.OpenAsyncSession())
			{
				await session.StoreAsync(new Info());
				await session.SaveChangesAsync();
			}
		}
	}
}