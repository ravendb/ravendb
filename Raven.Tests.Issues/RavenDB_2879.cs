using System.Linq;
using System.Threading.Tasks;

using Raven.Client.Indexes;
using Raven.Tests.Helpers;

using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_2879 : RavenTestBase
	{
		public class User
		{
			public string Id
			{
				get;
				set;
			}

			public string Name
			{
				get;
				set;
			}
		}

		public class Users_FullName : AbstractTransformerCreationTask<User>
		{
			public class Result
			{
				public string Id
				{
					get;
					set;
				}

				public string Name
				{
					get;
					set;
				}
			}

			public Users_FullName()
			{
				TransformResults = users => from user in users
											select new
											{
												user.Id,
												user.Name,
											};
			}
		}

		[Fact]
		public async Task CanGetDocumentIdFromTransformer()
		{
			using (var store = NewDocumentStore())
			{
				new Users_FullName().Execute(store);

				using (var session = store.OpenAsyncSession())
				{
					await session.StoreAsync(new User { Name = "George" });
					await session.SaveChangesAsync();
				}

				using (var session = store.OpenAsyncSession())
				{
					var user = await session.LoadAsync<Users_FullName, Users_FullName.Result>("users/1");

					Assert.NotNull(user.Id);
				}
			}
		}
	}
}
