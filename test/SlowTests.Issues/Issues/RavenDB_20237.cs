using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_20237 : RavenTestBase
{
    public RavenDB_20237(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Encryption)]
    public async Task MustProvideEncryptionKeyToAllDbStorages()
    {
        var result = await Encryption.EncryptedServerAsync();

        using (var store = GetDocumentStore(new Options
               {
                   ModifyDatabaseName = _ => result.DatabaseName,
                   ClientCertificate = result.Certificates.ServerCertificate.Value,
                   AdminCertificate = result.Certificates.ServerCertificate.Value,
                   Encrypted = true
               }))
        {
            Index index = new Index();
            await index.ExecuteAsync(store);

            var database = await GetDatabase(result.DatabaseName);

            Assert.NotNull(database.MasterKey);

            Assert.True(database.ConfigurationStorage.Environment.Options.Encryption.IsEnabled);
            Assert.Equal(database.MasterKey, database.ConfigurationStorage.Environment.Options.Encryption.MasterKey);

            var indexInstance = database.IndexStore.GetIndex(index.IndexName);

            Assert.True(indexInstance._environment.Options.Encryption.IsEnabled);
            Assert.Equal(database.MasterKey, indexInstance._environment.Options.Encryption.MasterKey);
        }
    }

    private class Index : AbstractIndexCreationTask<User>
    {
        public Index()
        {
            Map = users => from u in users select new {u.Name};
        }
    }
}
