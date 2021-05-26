using System;
using System.IO;
using FastTests;
using Newtonsoft.Json;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Server.Storage.Schema.Updates.Documents;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_13468 : RavenTestBase
    {
        public RavenDB_13468(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanMigrateLegacyCountersWithMultipleDbIds()
        {
            From41016.NumberOfCountersToMigrateInSingleTransaction = 20;
            
            var jsonPath = Path.Combine(NewDataPath(forceCreateDir: true), "RavenDB_13468.counters-snapshot");
            ExtractFile(jsonPath, "SlowTests.Data.RavenDB_13468.counters-snapshot-document.json");
            dynamic json = LoadJson(jsonPath);
            var countersSnapshot = json[Raven.Client.Constants.Documents.Metadata.RevisionCounters];

            var backupPath = NewDataPath(forceCreateDir: true);
            var fullBackupPath = Path.Combine(backupPath, "RavenDB_13468.ravendb-snapshot");
            ExtractFile(fullBackupPath, "SlowTests.Data.RavenDB_13468.counters.test.4.1.6.ravendb-snapshot");

            using (var store = GetDocumentStore())
            {
                var databaseName = GetDatabaseName();

                using (Backup.RestoreDatabase(store, new RestoreBackupConfiguration
                {
                    BackupLocation = backupPath,
                    DatabaseName = databaseName
                }))
                {
                    var stats = store.Maintenance.ForDatabase(databaseName).Send(new GetStatisticsOperation());

                    Assert.Equal(1, stats.CountOfDocuments);
                    Assert.Equal(77, stats.CountOfCounterEntries);

                    using (var session = store.OpenSession(databaseName))
                    {
                        var counters = session.CountersFor("domainclaimchecks").GetAll();
                        Assert.Equal(2450, counters.Count);

                        foreach (var prop in countersSnapshot)
                        {
                            var name = (string)prop.Name;
                            Assert.Contains(name, counters.Keys);
                        }

                        foreach (var counter in counters)
                        {
                            var name = counter.Key;
                            var value = counter.Value;

                            Assert.Contains(name, countersSnapshot);
                            Assert.Equal((long)countersSnapshot[name], value);

                        }
                    }

                    using (var session = store.OpenSession(databaseName))
                    {
                        var rand = new Random(357);
                        var counters = session.CountersFor("domainclaimchecks");
                        for (int i = 0; i < 3000; i++)
                        {
                            counters.Increment(rand.Next(0, 10000).ToString());
                        }
                        session.SaveChanges();
                    }

                    using (var session = store.OpenSession(databaseName))
                    {
                        var counters = session.CountersFor("domainclaimchecks").GetAll();

                        foreach (var counter in counters)
                        {
                            var name = counter.Key;
                            var value = counter.Value;

                            if (countersSnapshot[name] != null)
                                Assert.True((long)countersSnapshot[name] == value, $"{name} is has {value} but expects {(long)countersSnapshot[name]}");

                        }
                    }
                    WaitForUserToContinueTheTest(store);

                }

            }
        }

        public static void ExtractFile(string path, string assemblyName)
        {
            using (var file = File.Create(path))
            using (var stream = typeof(RavenDB_10404).Assembly.GetManifestResourceStream(assemblyName))
            {
                stream.CopyTo(file);
            }
        }

        private static object LoadJson(string path)
        {
            using (StreamReader r = new StreamReader(path))
            {
                string json = r.ReadToEnd();
                return JsonConvert.DeserializeObject<object>(json);
            }
        }
    }
}
