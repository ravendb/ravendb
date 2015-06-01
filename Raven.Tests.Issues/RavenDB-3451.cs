using System.Threading.Tasks;
using Raven.Tests.Helpers;
using Raven.Tests.MailingList;
using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_3451 : RavenTestBase
	{
		[Fact]

		public async Task GetMetadataForAsyncForAsyncSession()
		{
			using(var store = NewDocumentStore())
			{
				using(var session = store.OpenAsyncSession())
				{
					var entity = new User {Name = "John", Email = "Johnson@gmail.com"};
					await session.StoreAsync(entity);
					await session.SaveChangesAsync();

					 var metaData =  await session.Advanced.GetMetadataForAsync(entity);
				
					 Assert.NotNull(metaData);
				}
			}
		}

		public class User
		{
			public string Name  { get; set; }
			public string Email { get; set; }
		}
	}
}

