using System.Linq;
using Raven.Abstractions.Commands;
using Raven.Abstractions.Data;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class BatchPatching : RavenTest
	{
		[Fact]
		public void CanSuccessulyPatchInBatches()
		{
			using(var store = NewDocumentStore())
			{
				const int count = 512;
				using(var s = store.OpenSession())
				{
					for (int i = 0; i < count; i++)
					{
						s.Store(new User
						{
							Id = "users/"+i,
							Age = i,
						});
					}
					s.SaveChanges();
				}

				store.DatabaseCommands.Batch(
					Enumerable.Range(0, count/2).Select(i => new PatchCommandData
					{
						Key = "users/" + i,
						Patches = new[]
						{
							new PatchRequest
							{
								Name = "Name",
								Value = "Users-" + i
							},
						}
					}).ToArray());

				store.DatabaseCommands.Batch(
					Enumerable.Range(count / 2, count).Select(i => new PatchCommandData
					{
						Key = "users/" + i,
						Patches = new PatchRequest[]
						{
							new PatchRequest
							{
								Name = "Name",
								Value = "Users-" + i
							},
						}
					}).ToArray());

				using (var s = store.OpenSession())
				{
					s.Advanced.MaxNumberOfRequestsPerSession = count + 2;
					for (int i = 0; i < count; i++)
					{
						Assert.Equal("Users-"+i, s.Load<User>("users/"+i).Name);
					}
				}

			}
		}
	}
}