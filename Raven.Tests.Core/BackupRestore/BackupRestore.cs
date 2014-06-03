using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Client.Connection;
using Raven.Client.Document;
using Raven.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace Raven.Tests.Core.BackupRestore
{
    public class BackupRestore : RavenCoreTestBase
    {
        [Fact]
        public void CanBackupAndRestore()
        {
            const string BackupPath = "C:\\";

            using (var source = GetDocumentStore())
            {
                var req = source.JsonRequestFactory.CreateHttpJsonRequest(
                    new CreateHttpJsonRequestParams(null, source.Url + "/admin/backup", "POST", new OperationCredentials(null, source.Credentials), source.Conventions)
                    );

                var backupRequest = new
                {
                    BackupLocation = BackupPath.Replace("\\", "\\\\"),
                    DatabaseDocument = new DatabaseDocument { Id = Constants.SystemDatabase }
                };

                var json = RavenJObject.FromObject(backupRequest).ToString();
                req.WriteAsync(json).Wait();

                WaitForBackup(source);
            }
        }

        private void WaitForBackup(DocumentStore store)
        {
            BackupStatus status = null;
            var messagesSeenSoFar = new HashSet<BackupStatus.BackupMessage>();

            while (status == null)
            {
                Thread.Sleep(100); // Allow the server to process the request
                status = GetStatusDoc(store);
            }

            while (status.IsRunning)
            {
                Thread.Sleep(1000);
                status = GetStatusDoc(store);
            }
        }

        private BackupStatus GetStatusDoc(DocumentStore store)
        {
            var req = store.JsonRequestFactory.CreateHttpJsonRequest(
                    new CreateHttpJsonRequestParams(null, store.Url + "/docs/" + BackupStatus.RavenBackupStatusDocumentKey, "GET", new OperationCredentials(null, store.Credentials), store.Conventions)
                    );

            var json = (RavenJObject)req.ReadResponseJson();
            return json.Deserialize<BackupStatus>(store.Conventions);
        }
    }
}
