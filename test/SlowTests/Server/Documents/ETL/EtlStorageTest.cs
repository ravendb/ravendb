using System.Collections.Generic;
using Raven.Server.Documents.ETL.Providers.Raven;
using Xunit;

namespace SlowTests.Server.Documents.ETL
{
    public class EtlStorageTest : EtlTestBase
    {
        [Fact]
        public void Can_store_update_and_remove_last_processed_etag()
        {
            using (var store = GetDocumentStore())
            {
                var database = GetDatabase(store.Database).Result;

                var dest = new RavenDestination
                {
                    Database = "db1",
                    Url = "http://127.0.0.1",
                };

                var errors = new List<string>();
                dest.Validate(ref errors);

                Assert.Equal(0, errors.Count);

                var etlStorage = database.ConfigurationStorage.EtlStorage;

                Assert.Equal(0, etlStorage.GetLastProcessedEtag(dest, "transformation1"));

                etlStorage.StoreLastProcessedEtag(dest, "transformation1", 5);
                Assert.Equal(5, etlStorage.GetLastProcessedEtag(dest, "transformation1"));

                etlStorage.StoreLastProcessedEtag(dest, "transformation2", 3);
                Assert.Equal(3, etlStorage.GetLastProcessedEtag(dest, "transformation2"));

                etlStorage.StoreLastProcessedEtag(dest, "transformation1", 10);
                Assert.Equal(10, etlStorage.GetLastProcessedEtag(dest, "transformation1"));

                etlStorage.Remove(dest, "transformation1");
                Assert.Equal(0, etlStorage.GetLastProcessedEtag(dest, "transformation1"));
                Assert.Equal(3, etlStorage.GetLastProcessedEtag(dest, "transformation2"));

                etlStorage.Remove(dest);
                Assert.Equal(0, etlStorage.GetLastProcessedEtag(dest, "transformation1"));
                Assert.Equal(0, etlStorage.GetLastProcessedEtag(dest, "transformation2"));
            }
        }
    }
}