using System;
using System.Reflection;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Smuggler;
using Tests.Infrastructure;
using Xunit;

namespace FastTests;

public partial class RavenTestBase
{
    public readonly SamplesTestBase Samples;

    public class SamplesTestBase
    {
        private readonly RavenTestBase _parent;

        public SamplesTestBase(RavenTestBase parent)
        {
            _parent = parent ?? throw new ArgumentNullException(nameof(parent));
        }

        public void CreateNorthwindDatabase(IDocumentStore store, DatabaseItemType operateOnTypes = DatabaseItemType.Documents)
        {
            store.Maintenance.Send(new CreateSampleDataOperation(operateOnTypes));
        }

        public async Task CreateLegacyNorthwindDatabaseAsync(IDocumentStore store)
        {
            await using (var stream = Assembly.GetExecutingAssembly().GetManifestResourceStream("Tests.Infrastructure.Data.Northwind.4.2.ravendbdump"))
            {
                Assert.NotNull(stream);

                var operation = await store.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), stream);
                await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));
            }
        }
    }
}
