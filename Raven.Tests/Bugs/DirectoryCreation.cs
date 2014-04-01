using System.IO;
using System.Linq;
using Raven.Database.Extensions;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Bugs
{
	public class DirectoryCreation : RavenTest
	{
		[Fact]
		public void ShouldOnlyBeInDataDir()
		{
		    var appDataPath = NewDataPath("App_Data");
            var dataPath = NewDataPath("Data");

            IOExtensions.DeleteDirectory(appDataPath);
            IOExtensions.DeleteDirectory(dataPath);

            Assert.False(Directory.Exists(appDataPath));
            Assert.False(Directory.Exists(dataPath));

            using (var store = NewDocumentStore(dataDir: appDataPath, runInMemory: false))
            {
                using (var session = store.OpenSession())
                {
                    string someEmail = "e@d.com";
                    session.Query<User>().FirstOrDefault(u => u.Email == someEmail);
                    session.Store(new User { Email = "e@d.com" });
                    session.SaveChanges();
                    session.Query<User>()
                           .Customize(x => x.WaitForNonStaleResultsAsOfNow())
                           .Single(u => u.Email == someEmail);
                }

                Assert.True(Directory.Exists(appDataPath));
                Assert.False(Directory.Exists(dataPath));
            }

            IOExtensions.DeleteDirectory(appDataPath);
            IOExtensions.DeleteDirectory(dataPath);
		}
	}
}