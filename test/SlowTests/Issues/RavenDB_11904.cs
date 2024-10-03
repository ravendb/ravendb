﻿using System;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Smuggler;
using Raven.Client.ServerWide;
using Sparrow.Logging;
using Tests.Infrastructure;
using Voron.Recovery;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_11904 : RavenTestBase
    {
        public RavenDB_11904(ITestOutputHelper output) : base(output)
        {
        }

        [RavenMultiplatformFact(RavenTestCategory.Voron, RavenArchitecture.AllX64)]
        public Task CanLoadDatabaseAfterUsingVoronRecoveryOnItWithCopyOnWriteMode()
        {
            return CanLoadDatabaseAfterUsingVoronRecoveryOnItWithCopyOnWriteMode(false);
        }

        [RavenMultiplatformFact(RavenTestCategory.Voron, RavenArchitecture.AllX64)]
        public Task CanLoadDatabaseAfterUsingVoronRecoveryOnItWithCopyOnWriteModeCompressed()
        {
            return CanLoadDatabaseAfterUsingVoronRecoveryOnItWithCopyOnWriteMode(true);
        }

        private async Task CanLoadDatabaseAfterUsingVoronRecoveryOnItWithCopyOnWriteMode(bool compressed)
        {
            var dbPath = NewDataPath(prefix: Guid.NewGuid().ToString());
            var recoveryExportPath = NewDataPath(prefix: Guid.NewGuid().ToString());

            DatabaseStatistics databaseStatistics;

            // create db with sample data
            using (var store = GetDocumentStore(new Options()
            {
                Path = dbPath,
                ModifyDatabaseRecord = record =>
                {
                    if (compressed)
                    {
                        record.DocumentsCompression = new DocumentsCompressionConfiguration
                        {
                            Collections = new[] { "Orders", "Employees", "Companies", "Products" },
                            CompressRevisions = true
                        };
                    }
                }
            }))
            {
                await Samples.CreateLegacyNorthwindDatabaseAsync(store);

                databaseStatistics = store.Maintenance.Send(new GetStatisticsOperation());
            }

            var journals = new DirectoryInfo(Path.Combine(dbPath, "Journals")).GetFiles();

            // run recovery
            using (var recovery = new Recovery(new VoronRecoveryConfiguration()
            {
                LoggingLevel = LogLevel.Off,
                DataFileDirectory = dbPath,
                PathToDataFile = Path.Combine(dbPath, "Raven.voron"),
                OutputFileName = Path.Combine(recoveryExportPath, "recovery.ravendump"),
            }))
            {
                recovery.Execute(TextWriter.Null, CancellationToken.None);
            }

            // make sure no journal file was lost during the process - by default we use copy on write mode
            var journalsAfterRecovery = new DirectoryInfo(Path.Combine(dbPath, "Journals")).GetFiles();

            Assert.Equal(journals.Length, journalsAfterRecovery.Length);
            Assert.True(journals.All(x => journalsAfterRecovery.Any(y => y.Name == x.Name)));

            // let's open the database
            using (var store = GetDocumentStore(new Options()
            {
                Path = dbPath
            }))
            {
                var currentStats = store.Maintenance.Send(new GetStatisticsOperation());

                Assert.Equal(databaseStatistics.CountOfAttachments, currentStats.CountOfAttachments);
                Assert.Equal(databaseStatistics.CountOfDocuments, currentStats.CountOfDocuments);
            }

            // let's import the recovery files

            using (var store = GetDocumentStore())
            {
                var op = await store.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions()
                {

                }, Path.Combine(recoveryExportPath, "recovery-2-Documents.ravendump"));

                op.WaitForCompletion(TimeSpan.FromMinutes(2));

                var currentStats = store.Maintenance.Send(new GetStatisticsOperation());

                // + 1 as recovery adds some artificial items
                Assert.Equal(databaseStatistics.CountOfAttachments + 1, currentStats.CountOfAttachments);
                Assert.Equal(databaseStatistics.CountOfDocuments + 1, currentStats.CountOfDocuments);
            }
        }
    }
}
