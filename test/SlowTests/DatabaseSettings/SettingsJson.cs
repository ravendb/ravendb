using System.IO;
using FastTests;
using Raven.Server.Config;
using Raven.Server.Config.Settings;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.DataBaseSettings
{
    public class SettingsJson : RavenTestBase
    {
        public SettingsJson(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanParseNestedJsonObjectsFromSettingsJsonFile()
        {
            DoNotReuseServer();

            string settingsJsonFile = default;

            try
            {
                const string jsonWithNestedContent = @"{
                                                         ""ETL.MaxBatchSizeInMb"":77,
                                                         ""ETL"":
                                                         {
                                                            ""MaxNumberOfExtractedItems"":222,
                                                            ""MaxNumberOfExtractedDocuments"":333
                                                         },
                                                         ""ETL"":
                                                         {
                                                            ""SQL"":
                                                            {
                                                               ""CommandTimeoutInSec"":444
                                                            }
                                                         }
                                                      }";

                settingsJsonFile = Path.Combine($"{Path.GetDirectoryName(GetTempFileName())}", "settings.json");
                File.WriteAllText(settingsJsonFile, jsonWithNestedContent);

                var serverConfig = RavenConfiguration.CreateForServer(null, settingsJsonFile);
                serverConfig.Initialize();

                var databaseConfig = RavenConfiguration.CreateForDatabase(serverConfig, "dbName");
                databaseConfig.Initialize();

                Assert.Equal("77 MBytes", serverConfig.Etl.MaxBatchSize.ToString());
                Assert.Equal(222, serverConfig.Etl.MaxNumberOfExtractedItems);
                Assert.Equal(333, serverConfig.Etl.MaxNumberOfExtractedDocuments);
                Assert.Equal(444, serverConfig.Etl.SqlCommandTimeout.Value.GetValue(TimeUnit.Seconds));

                Assert.Equal("77 MBytes", databaseConfig.Etl.MaxBatchSize.ToString());
                Assert.Equal(222, databaseConfig.Etl.MaxNumberOfExtractedItems);
                Assert.Equal(333, databaseConfig.Etl.MaxNumberOfExtractedDocuments);
                Assert.Equal(444, databaseConfig.Etl.SqlCommandTimeout.Value.GetValue(TimeUnit.Seconds));
            }
            finally
            {
                File.Delete(settingsJsonFile);
            }
        }
    }
}
