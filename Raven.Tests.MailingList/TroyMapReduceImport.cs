using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Smuggler;
using Raven.Database.Smuggler;
using Raven.Tests.Common;

using Xunit;
using System.Linq;

namespace Raven.Tests.MailingList
{
    public class TroyMapReduceImport : RavenTest
    {
        [Fact]
        public async Task CanGetCorrectResult()
        {
            using (var store = NewDocumentStore())
            {
                var dataDumper = new DatabaseDataDumper(store.SystemDatabase);
                using (var stream = typeof(TroyMapReduceImport).Assembly.GetManifestResourceStream("Raven.Tests.MailingList.Sandbox.ravendbdump"))
                {
                    await dataDumper.ImportData(new SmugglerImportOptions<RavenConnectionStringOptions> { FromStream = stream });
                }

                using(var s = store.OpenSession())
                {
                    var objects = s.Query<object>("LogEntry/CountByDate")
                        .Customize(x => x.WaitForNonStaleResults())
                        .ToList();
                    Assert.Equal(4, objects.Count);
                }
            }
        }

        [Fact]
        public async Task CanGetCorrectResult_esent()
        {
            using (var store = NewDocumentStore(requestedStorage: "esent"))
            {
                var dataDumper = new DatabaseDataDumper(store.SystemDatabase);
                using (var stream = typeof(TroyMapReduceImport).Assembly.GetManifestResourceStream("Raven.Tests.MailingList.Sandbox.ravendbdump"))
                {
                    await dataDumper.ImportData(new SmugglerImportOptions<RavenConnectionStringOptions> { FromStream = stream });
                }

                WaitForUserToContinueTheTest(store);
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
