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
                var serverConfig1 = RavenConfiguration.CreateForServer(null);
                settingsJsonFile = $"{Path.GetDirectoryName(serverConfig1.ConfigPath)}\\settings.json";
                
                const string jsonWithNestedContent = "{" +
                                                     "\"ETL.MaxBatchSizeInMb\":77," +
                                                     "\"ETL\":" +
                                                     "{" +
                                                         "\"MaxNumberOfExtractedItems\":222," +
                                                         "\"MaxNumberOfExtractedDocuments\":333" +
                                                     "}," +
                                                     "\"ETL\":" +
                                                     "{" +
                                                         "\"SQL\":" +
                                                         "{" +
                                                             "\"CommandTimeoutInSec\":444" +
                                                         "}" +
                                                     "}" +
                                                     "}";

                File.WriteAllText(settingsJsonFile, jsonWithNestedContent);

                var serverConfig2 = RavenConfiguration.CreateForServer(null);
                serverConfig2.Initialize();
                
                var databaseConfig = RavenConfiguration.CreateForDatabase(serverConfig2, "dbName");
                databaseConfig.Initialize();

                Assert.Equal("77 MBytes", serverConfig2.Etl.MaxBatchSize.ToString());
                Assert.Equal(222, serverConfig2.Etl.MaxNumberOfExtractedItems);
                Assert.Equal(333, serverConfig2.Etl.MaxNumberOfExtractedDocuments);
                Assert.Equal(444, serverConfig2.Etl.SqlCommandTimeout.Value.GetValue(TimeUnit.Seconds));
               
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
