using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Smuggler;
using Raven.Database.Smuggler;
using Raven.Tests.Common;

using Xunit;
using System.Linq;
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
                var dataDumper = new DatabaseDataDumper(store.SystemDatabase);
                using (var stream = typeof(TroyMapReduceImport).Assembly.GetManifestResourceStream("Raven.Tests.MailingList.Sandbox.ravendump"))
                {
                    await dataDumper.ImportData(new SmugglerImportOptions<RavenConnectionStringOptions> { FromStream = stream });
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