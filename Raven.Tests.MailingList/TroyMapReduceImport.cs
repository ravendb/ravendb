using System.Threading.Tasks;

using Raven.Tests.Common;

using Xunit;
using System.Linq;

using Raven.Abstractions.Database.Smuggler.Database;
using Raven.Database.Smuggler.Embedded;
using Raven.Smuggler.Database;
using Raven.Smuggler.Database.Streams;

using Xunit.Extensions;

namespace Raven.Tests.MailingList
{
	public class TroyMapReduceImport : RavenTest
	{
        [Theory]
        [PropertyData("Storages")]
        public async Task CanGetCorrectResult(string storage)
        {
            using (var store = NewDocumentStore(requestedStorage: storage))
            {
                using (var stream = typeof(TroyMapReduceImport).Assembly.GetManifestResourceStream("Raven.Tests.MailingList.Sandbox.ravendump"))
                {
                    var smuggler = new DatabaseSmuggler(
                        new DatabaseSmugglerOptions(),
                        new DatabaseSmugglerStreamSource(stream),
                        new DatabaseSmugglerEmbeddedDestination(store.SystemDatabase));

                    await smuggler.ExecuteAsync();
                }

                using (var s = store.OpenSession())
                {
                    var objects = s.Query<object>("LogEntry/CountByDate")
                        .Customize(x => x.WaitForNonStaleResults())
                        .ToList();
                    Assert.Equal(4, objects.Count);
                }
            }
        } 
	}
}