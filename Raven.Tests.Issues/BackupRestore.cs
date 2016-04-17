//-----------------------------------------------------------------------
// <copyright file="BackupRestore.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Client.Indexes;
using Raven.Database;
using Raven.Database.Actions;
using Raven.Database.Config;
using Raven.Database.Extensions;
using Raven.Json.Linq;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Storage
{
    public class BackupRestore : RavenTest
    {
        private readonly string DataDir;
        private readonly string BackupDir;
        private DocumentDatabase db;

        public BackupRestore()
        {
            BackupDir = NewDataPath("BackupDatabase");
            DataDir = NewDataPath("DataDirectory");

            db = new DocumentDatabase(new RavenConfiguration
            {
                DataDirectory = DataDir,
                RunInUnreliableYetFastModeThatIsNotSuitableForProduction = false
            }, null);
            db.Indexes.PutIndex(new RavenDocumentsByEntityName().IndexName, new RavenDocumentsByEntityName().CreateIndexDefinition());
        }

        public override void Dispose()
        {
            db.Dispose();
            base.Dispose();
        }

        [Fact]
        public void AfterBackupRestoreCanReadDocument()
        {
            db.Documents.Put("ayende", null, RavenJObject.Parse("{'email':'ayende@ayende.com'}"), new RavenJObject(), null);

            db.Maintenance.StartBackup(BackupDir, false, new DatabaseDocument(), new ResourceBackupState());
            WaitForBackup(db, true);

            db.Dispose();
            IOExtensions.DeleteDirectory(DataDir);

            MaintenanceActions.Restore(new RavenConfiguration(), new DatabaseRestoreRequest
            {
                BackupLocation = BackupDir,
                DatabaseLocation = DataDir,
                Defrag = true
            }, s => { });

            db = new DocumentDatabase(new RavenConfiguration { DataDirectory = DataDir }, null);

            var document = db.Documents.Get("ayende", null);
            Assert.NotNull(document);

            var jObject = document.ToJson();
            Assert.Equal("ayende@ayende.com", jObject.Value<string>("email"));
        }


        [Fact]
        public void AfterBackupRestoreCanQueryIndex_CreatedAfterRestore()
        {
            db.Documents.Put("ayende", null, RavenJObject.Parse("{'email':'ayende@ayende.com'}"), RavenJObject.Parse("{'Raven-Entity-Name':'Users'}"), null);

            db.Maintenance.StartBackup(BackupDir, false, new DatabaseDocument(), new ResourceBackupState());
            WaitForBackup(db, true);

            db.Dispose();
            IOExtensions.DeleteDirectory(DataDir);

            MaintenanceActions.Restore(new RavenConfiguration(), new DatabaseRestoreRequest
            {
                BackupLocation = BackupDir,
                DatabaseLocation = DataDir
            }, s => { });

            db = new DocumentDatabase(new RavenConfiguration { DataDirectory = DataDir }, null);
            db.SpinBackgroundWorkers();
            QueryResult queryResult;
            do
            {
                queryResult = db.Queries.Query("Raven/DocumentsByEntityName", new IndexQuery
                {
                    Query = "Tag:[[Users]]",
                    PageSize = 10
                }, CancellationToken.None);
            } while (queryResult.IsStale);
            Assert.Equal(1, queryResult.Results.Count);
        }

        [Fact]
        public void AfterBackupRestoreCanQueryIndex_CreatedBeforeRestore()
        {
            db.Documents.Put("ayende", null, RavenJObject.Parse("{'email':'ayende@ayende.com'}"), RavenJObject.Parse("{'Raven-Entity-Name':'Users'}"), null);
            db.SpinBackgroundWorkers();
            QueryResult queryResult;
            do
            {
                queryResult = db.Queries.Query("Raven/DocumentsByEntityName", new IndexQuery
                {
                    Query = "Tag:[[Users]]",
                    PageSize = 10
                }, CancellationToken.None);
            } while (queryResult.IsStale);
            Assert.Equal(1, queryResult.Results.Count);

            db.Maintenance.StartBackup(BackupDir, false, new DatabaseDocument(), new ResourceBackupState());
            WaitForBackup(db, true);

            db.Dispose();
            IOExtensions.DeleteDirectory(DataDir);

            MaintenanceActions.Restore(new RavenConfiguration(), new DatabaseRestoreRequest
            {
                BackupLocation = BackupDir,
                DatabaseLocation = DataDir,
                Defrag = true
            }, s => { });

            db = new DocumentDatabase(new RavenConfiguration { DataDirectory = DataDir }, null);

            queryResult = db.Queries.Query("Raven/DocumentsByEntityName", new IndexQuery
            {
                Query = "Tag:[[Users]]",
                PageSize = 10
            }, CancellationToken.None);
            Assert.Equal(1, queryResult.Results.Count);
        }

        [Fact]
        public void AfterFailedBackupRestoreCanDetectError()
        {
            db.Documents.Put("ayende", null, RavenJObject.Parse("{'email':'ayende@ayende.com'}"), RavenJObject.Parse("{'Raven-Entity-Name':'Users'}"), null);
            db.SpinBackgroundWorkers();
            QueryResult queryResult;
            do
            {
                queryResult = db.Queries.Query("Raven/DocumentsByEntityName", new IndexQuery
                {
                    Query = "Tag:[[Users]]",
                    PageSize = 10
                }, CancellationToken.None);
            } while (queryResult.IsStale);
            Assert.Equal(1, queryResult.Results.Count);

            File.WriteAllText("raven.db.test.backup.txt", "Sabotage!");
            db.Maintenance.StartBackup("raven.db.test.backup.txt", false, new DatabaseDocument(), new ResourceBackupState());
            WaitForBackup(db, false);

            var condition = GetStateOfLastStatusMessage().Severity == BackupStatus.BackupMessageSeverity.Error;
            Assert.True(condition);
        }

        [Fact]
        public void AfterBackupRestore_IndexConsistentWithWritesDuringBackup()
        {
            var count = 1;
            var docId = string.Format("ayende{0}", count++.ToString("D4"));
            db.Documents.Put(docId, null, RavenJObject.Parse("{'email':'ayende@ayende.com'}"), RavenJObject.Parse("{'Raven-Entity-Name':'Users'}"), null);
            db.SpinBackgroundWorkers();

            QueryResult queryResult;
            do
            {
                queryResult = db.Queries.Query("Raven/DocumentsByEntityName", new IndexQuery
                {
                    Query = "Tag:[[Users]]",
                    PageSize = 10
                }, CancellationToken.None);
            } while (queryResult.IsStale);
            Assert.Equal(1, queryResult.Results.Count);

            var runInserts = true;
            Task.Run(() =>
            {
                while (runInserts)
                {
                    docId = string.Format("ayende{0}", count++.ToString("D4"));
                    db.Documents.Put(docId, null, RavenJObject.Parse("{'email':'ayende@ayende.com'}"), RavenJObject.Parse("{'Raven-Entity-Name':'Users'}"), null);
                    db.IndexStorage.FlushMapIndexes();
                }
            });

            db.Maintenance.StartBackup(BackupDir, false, new DatabaseDocument(), new ResourceBackupState());
            WaitForBackup(db, true);
            runInserts = false;

            db.Dispose();
            IOExtensions.DeleteDirectory(DataDir);

            MaintenanceActions.Restore(new RavenConfiguration(), new DatabaseRestoreRequest
            {
                BackupLocation = BackupDir,
                DatabaseLocation = DataDir,
                Defrag = true
            }, s => { });

            db = new DocumentDatabase(new RavenConfiguration { DataDirectory = DataDir }, null);
            docId = string.Format("ayende{0}", count++.ToString("D4"));
            db.Documents.Put(docId, null, RavenJObject.Parse("{'email':'ayende@ayende.com'}"), RavenJObject.Parse("{'Raven-Entity-Name':'Users'}"), null);
            db.SpinBackgroundWorkers();

            int next = 0;
            var storedDocs = new List<string>();

            while (true)
            {
                var batch = db.Documents.GetDocumentsWithIdStartingWith("ayende", null, null, next, 1024, CancellationToken.None, ref next);
                storedDocs.AddRange(batch.Select(doc => doc.Value<RavenJObject>("@metadata").Value<string>("@id")));
                if (batch.Length < 1024) break;
            }

            List<string> indexedDocs;
            bool stale;
            do
            {
                indexedDocs = db.Queries.QueryDocumentIds("Raven/DocumentsByEntityName", new IndexQuery
                {
                    Query = "Tag:[[Users]]",
                    PageSize = int.MaxValue,
                    WaitForNonStaleResultsAsOfNow = true
                }, new CancellationTokenSource(), out stale).ToList();
            } while (stale);

            if (storedDocs.Count != indexedDocs.Count)
            {
                var storedHash = new HashSet<string>(storedDocs);
                var indexedHash = new HashSet<string>(indexedDocs);
                foreach (var id in storedDocs.Union(indexedDocs).OrderBy(x => x))
                {
                    Debug.WriteLine("{0} Database:{1} Indexed:{2}", id, storedHash.Contains(id), indexedHash.Contains(id));
                }
            }

            Assert.Equal(storedDocs.Count, indexedDocs.Count());
            db.Dispose();
        }

        private BackupStatus.BackupMessage GetStateOfLastStatusMessage()
        {
            JsonDocument jsonDocument = db.Documents.Get(BackupStatus.RavenBackupStatusDocumentKey, null);
            var backupStatus = jsonDocument.DataAsJson.JsonDeserialization<BackupStatus>();
            return backupStatus.Messages.Last();
        }
    }
}
