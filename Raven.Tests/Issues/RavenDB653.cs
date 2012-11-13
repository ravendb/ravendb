using Raven.Abstractions.Data;
using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB653 : RavenTest
	{
		[Fact]
		public void CorruptingDocs()
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new User
					{
						Name = "abcd"
					});
					session.SaveChanges();
				}

				store.DatabaseCommands.Patch("users/1",new ScriptedPatchRequest
				{
					Script = @"this[""""] = 10;"
				});

				using (var session = store.OpenSession())
				{
					session.Load<User>(1);
				}

			}
		}
		public class User
		{
			public string Name { get; set; }
		}
	}
}