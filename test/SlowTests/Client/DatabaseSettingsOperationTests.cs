using System.Collections.Generic;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.ServerWide.Operations;
using Xunit;
using Xunit.Abstractions;
using Raven.Client.ServerWide.Operations.Configuration;
using Tests.Infrastructure;

namespace SlowTests.Client
{
    public class DatabaseSettingsOperationTests : RavenTestBase
    {
        public DatabaseSettingsOperationTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Configuration)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public void CheckIfConfigurationSettingsIsEmpty(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                CheckIfOurValuesGotSaved(store, new Dictionary<string, string>());
            }
        }

        [RavenTheory(RavenTestCategory.Configuration)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public void ChangeSingleSettingKeyOnServer(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var name = "Storage.PrefetchResetThresholdInGb";
                var value = "10";
                var settings = new Dictionary<string, string>();
                settings.Add(name, value);
                PutConfigurationSettings(store, settings);
                CheckIfOurValuesGotSaved(store, settings);
            }
        }

        [RavenTheory(RavenTestCategory.Configuration)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public void ChangeMultipleSettingsKeysOnServer(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                string[] names = { "Storage.PrefetchResetThresholdInGb", "Storage.TimeToSyncAfterFlushInSec", "Tombstones.CleanupIntervalInMin" };
                string[] values = { "10", "35", "10" };
                var settings = new Dictionary<string, string>();
                for (int i = 0; i < names.Length; ++i)
                    settings.Add(names[i], values[i]);
                PutConfigurationSettings(store, settings);
                CheckIfOurValuesGotSaved(store, settings);
            }
        }

        private static void PutConfigurationSettings(DocumentStore store, Dictionary<string, string> settings)
        {
            store.Maintenance.Send(new PutDatabaseSettingsOperation(store.Database, settings));
            store.Maintenance.Server.Send(new ToggleDatabasesStateOperation(store.Database, true));
            store.Maintenance.Server.Send(new ToggleDatabasesStateOperation(store.Database, false));
        }

        private static DatabaseSettings GetConfigurationSettings(DocumentStore store)
        {
            var settings = store.Maintenance.Send(new GetDatabaseSettingsOperation(store.Database));
            Assert.NotNull(settings);
            return settings;
        }

        private static void CheckIfOurValuesGotSaved(DocumentStore store, Dictionary<string, string> data)
        {
            var settings = GetConfigurationSettings(store);
            foreach (var item in data)
            {
                var configurationValue = settings?.Settings[item.Key];
                Assert.NotNull(configurationValue);
                Assert.Equal(item.Value, configurationValue);
            }
        }
    }
}
