using System.IO;
using System.Linq;
using Raven.Client.Embedded;
using Raven.Database.Extensions;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class DirectoryCreation
	{
		[Fact]
		public void ShouldOnlyBeInDataDir()
		{
			IOExtensions.DeleteDirectory("App_Data");
			IOExtensions.DeleteDirectory("Data");

			Assert.False(Directory.Exists("App_Data"));
			Assert.False(Directory.Exists("Data"));

			using (var store = new EmbeddableDocumentStore {DataDirectory = "App_Data"}.Initialize())
			{
				using (var session = store.OpenSession())
				{
					string someEmail = "e@d.com";
					session.Query<User>().FirstOrDefault(u => u.Email == someEmail);
					session.Store(new User {Email = "e@d.com"});
					session.SaveChanges();
					session.Query<User>()
					       .Customize(x => x.WaitForNonStaleResultsAsOfNow())
						   .Single(u => u.Email == someEmail);
				}
			}

			Assert.True(Directory.Exists("App_Data"));
			Assert.False(Directory.Exists("Data"));

			IOExtensions.DeleteDirectory("App_Data");
			IOExtensions.DeleteDirectory("Data");
		}
	}
}