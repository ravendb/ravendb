using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using FastTests.Utils;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Sparrow.Logging;
using Tests.Infrastructure;
using Voron;
using Voron.Recovery;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Voron
{
    public class RecoveryTest : ReplicationTestBase
    {
        public RecoveryTest(ITestOutputHelper output) : base(output)
        { }

        private class Entity
        {
            public string Name;
            public int Number;
        }

        [Fact64Bit]
        public async Task FullRecoverDatabaseWithDocsRevsAttachmentsCountersConflicts()
        {
            var rnd = new Random(123);
            var dbPath = NewDataPath(prefix: Guid.NewGuid().ToString());
            using var store = GetDocumentStore(new Options()
            {
                Path = dbPath,
                ModifyDatabaseRecord = record =>
                {
                    record.ConflictSolverConfig = new ConflictSolver
                    {
                        ResolveToLatest = false,
                        ResolveByCollection = new Dictionary<string, ScriptResolver>()
                    };
                }
            });

            var database = await GetDocumentDatabaseInstanceFor(store, store.Database);
            var type = database.GetAllStoragesEnvironment().Single(t => t.Type == StorageEnvironmentWithType.StorageEnvironmentType.Documents);
            var env = type.Environment;
            env.Options.ManualFlushing = true;

            using (var session = store.OpenSession())
            {
                session.Store(new Entity {Name = "Simple Document", Number = 1}, "doc/1");
                session.Store(new Entity {Name = "Simple with Attachment", Number = 2}, "doc/2");
                session.SaveChanges();
            }

            await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database);

            using (var session = store.OpenSession())
            {
                session.Store(new Entity {Name = "Simple with Attachments and will have revisions", Number = 3}, "doc/3");
                session.Store(new Entity {Name = "Simple with Overlapping Attachments and will have revisions", Number = 4}, "doc/4");
                session.Store(new Entity {Name = "Will be conflicted document", Number = 5}, "doc/5");
                session.Store(new Entity {Name = "Contains Counters", Number = 6}, "doc/6");
                session.SaveChanges();
            }

            var files = new List<string>();
            var randomArray = new byte[4096];
            for (int i = 0; i < 10; i++)
            {
                var file = Path.GetTempFileName();
                if (File.Exists(file))
                    File.Delete(file);
                File.WriteAllText(file, "This is a test file " + Guid.NewGuid() + Environment.NewLine);
                await using (var stream = File.AppendText(file))
                {
                    for (int j = 0; j < rnd.Next(1, 20); j++)
                    {
                        rnd.NextBytes(randomArray);
                        stream.Write(Encoding.UTF8.GetString(randomArray, 0, 4096 - rnd.Next(0, 25)));
                    }

                    stream.Flush();
                }

                files.Add(file);
            }

            var fileItem = files.ToArray();
            await using (var fs = File.Open(fileItem[0], FileMode.Open))
            using (var session = store.OpenSession())
            {
                session.Advanced.Attachments.Store("doc/2", "testAttachment.txt", fs, "text/txt");
                session.SaveChanges();
            }

            for (int i = 1; i <= 6; i++)
            {
                await using (var fs = File.Open(fileItem[i], FileMode.Open))
                using (var session = store.OpenSession())
                {
                    var entity = session.Load<Entity>("doc/3");
                    entity.Number = 10 + i;
                    session.Store(entity);
                    var filename = "samefilename.txt";
                    if (i == 3)
                        filename = "differentfilename.txt";
                    session.Advanced.Attachments.Store("doc/3", filename, fs, "text/txt");
                    session.SaveChanges();
                }
            }

            for (int i = 7; i < 12; i++)
            {
                var j = i - ((i >= 10) ? 3 : 0); // duplicate some files
                await using (var fs = File.Open(fileItem[j], FileMode.Open))
                using (var session = store.OpenSession())
                {
                    var entity = session.Load<Entity>("doc/4");
                    entity.Number = 10 + i;
                    session.Store(entity);
                    var filename = "samefilename.txt";
                    if (j == 9)
                        filename = "differentfilename.txt";
                    session.Advanced.Attachments.Store("doc/4", filename, fs, "text/txt");
                    session.SaveChanges();
                }
            }

            using var conflictingStore = GetDocumentStore(new Options()
            {
                Path = NewDataPath(prefix: Guid.NewGuid().ToString()),
                ModifyDatabaseRecord = record =>
                {
                    record.ConflictSolverConfig = new ConflictSolver {ResolveToLatest = false, ResolveByCollection = new Dictionary<string, ScriptResolver>()};
                }
            });
            using (var session = conflictingStore.OpenSession())
            {
                session.Store(new Entity {Name = "Conflicting doc", Number = 101}, "doc/5");
                session.Store(new Entity {Name = "ReplicationMarker", Number = 123}, "replicateMarker/1");
                session.SaveChanges();
            }

            using (var session = store.OpenSession())
            {
                var entity = session.Load<Entity>("doc/5");

                entity.Number = 100;

                session.SaveChanges();
            }

            using (var session = store.OpenSession())
            {
                session.CountersFor("doc/6").Increment("testCounter");
                session.SaveChanges();

                session.CountersFor("doc/6").Increment("testCounter");
                session.SaveChanges();
            }

            await SetupReplicationAsync(conflictingStore, store);
            await SetupReplicationAsync(store, conflictingStore);

            Assert.True(WaitForDocument(store, "replicateMarker/1"));


            var flushed = false;
            env.OnLogsApplied += () =>
            {
                flushed = true;
            };
            env.FlushLogToDataFile();
            Assert.True(flushed, "Test requires successful flush to log");

            string recoverDbName = $"RecoverDB_{Guid.NewGuid().ToString()}";
            var _ = store.Maintenance.Server.Send(new CreateDatabaseOperation(new DatabaseRecord {DatabaseName = recoverDbName}));
            using var x = EnsureDatabaseDeletion(recoverDbName, store);
            using var recoveredDatabase = await GetDatabase(recoverDbName);

            // run recovery
            var recoveryExportPath = NewDataPath(prefix: Guid.NewGuid().ToString());
            using (var recovery = new global::Voron.Recovery.Recovery(new VoronRecoveryConfiguration()
            {
                LoggingMode = LogMode.None,
                DataFileDirectory = dbPath,
                PathToDataFile = Path.Combine(dbPath, "Raven.voron"),
                LoggingOutputPath = recoveryExportPath,
                RecoveredDatabase = recoveredDatabase
            }))
            {
                var result = recovery.Execute(TextWriter.Null, CancellationToken.None);
            }

            WaitForUserToContinueTheTest(store);

            using (var recoveredSession = store.OpenSession(recoverDbName))
            {
                var doc1 = recoveredSession.Load<Entity>("doc/1");
                var doc2 = recoveredSession.Load<Entity>("doc/2");
                var doc3 = recoveredSession.Load<Entity>("doc/3");
                var doc4 = recoveredSession.Load<Entity>("doc/4");
                var doc6 = recoveredSession.Load<Entity>("doc/6");
                Assert.Throws<Raven.Client.Exceptions.Documents.DocumentConflictException>(() =>
                {
                    var doc5 = recoveredSession.Load<Entity>("doc/5");
                });
                var msg = "Failed to recover specific document: ";
                Assert.True(doc1 != null, $"{msg}doc/1");
                Assert.True(doc2 != null, $"{msg}doc/2");
                Assert.True(doc3 != null, $"{msg}doc/3");
                Assert.True(doc4 != null, $"{msg}doc/4");
                Assert.True(doc6 != null, $"{msg}doc/6");


                msg = "Invalid doc content: ";
                Assert.True("Simple Document".Equals(doc1.Name), $"{msg}doc/1");
                Assert.True("Simple with Attachment".Equals(doc2.Name), $"{msg}doc/2");
                Assert.True("Simple with Attachments and will have revisions".Equals(doc3.Name), $"{msg}doc/3");
                Assert.True("Simple with Overlapping Attachments and will have revisions".Equals(doc4.Name), $"{msg}doc/4");
                Assert.True("Contains Counters".Equals(doc6.Name), $"{msg}doc/6");
                Assert.True(doc1.Number == 1, $"{msg}doc/1");
                Assert.True(doc2.Number == 2, $"{msg}doc/2");
                Assert.True(doc3.Number == 16, $"{msg}doc/3");
                Assert.True(doc4.Number == 21, $"{msg}doc/4");

                var revisions1 = recoveredSession.Advanced.Revisions.GetFor<Entity>("doc/1");
                var revisions2 = recoveredSession.Advanced.Revisions.GetFor<Entity>("doc/2");
                var revisions3 = recoveredSession.Advanced.Revisions.GetFor<Entity>("doc/3");
                var revisions4 = recoveredSession.Advanced.Revisions.GetFor<Entity>("doc/4");

                msg = "Invalid number of revisions for: ";
                // because of the nature of recovery (stripping attachments, storing, and then reattaching) we may find more revisions then in the original db
                Assert.True(revisions1?.Count == 0, $"{msg}doc/1");
                Assert.True(revisions2?.Count >= 2, $"{msg}doc/2");
                Assert.True(revisions3?.Count >= 5, $"{msg}doc/3");
                Assert.True(revisions4?.Count >= 5, $"{msg}doc/4");

                msg = "Couldn't get recovered attachment: ";
                var attachment1 = recoveredSession.Advanced.Attachments.GetNames(doc1);
                var attachment2 = recoveredSession.Advanced.Attachments.GetNames(doc2);
                var attachment3 = recoveredSession.Advanced.Attachments.GetNames(doc3);
                var attachment4 = recoveredSession.Advanced.Attachments.GetNames(doc4);
                Assert.True(attachment1?.Length == 0, $"{msg}doc/1, count={attachment1?.Length}");
                Assert.True(attachment2?.Length == 1, $"{msg}doc/2, count={attachment2?.Length}");
                Assert.True(attachment3?.Length == 2, $"{msg}doc/3, count={attachment3?.Length}");
                Assert.True(attachment4?.Length == 2, $"{msg}doc/4, count={attachment4?.Length}");

                var counter6 = recoveredSession.CountersFor("doc/6")?.Get("testCounter");
                Assert.NotNull(counter6);
                Assert.Equal(counter6, 2L);
            }


            // we perform again the recovery just in order to cover some code that usually isn't covered in recover process 'alreadyExistingAttachments.Count > 0' @ void WriteDocumentInternal(Document document, IDocumentActions actions)
            using var recoveredDatabase2 = await GetDatabase(recoverDbName);
            using (var recovery = new global::Voron.Recovery.Recovery(new VoronRecoveryConfiguration()
            {
                LoggingMode = LogMode.None,
                DataFileDirectory = dbPath,
                PathToDataFile = Path.Combine(dbPath, "Raven.voron"),
                LoggingOutputPath = recoveryExportPath,
                RecoveredDatabase = recoveredDatabase2
            }))
            {
                var result = recovery.Execute(TextWriter.Null, CancellationToken.None);
            }
        }
    }
}
