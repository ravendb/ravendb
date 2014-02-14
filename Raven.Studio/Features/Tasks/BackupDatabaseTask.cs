using System;
using System.Collections.Generic;
using System.Net;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Documents;
using System.Windows.Ink;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Animation;
using System.Windows.Shapes;
using Raven.Abstractions.Data;
using Raven.Abstractions.Util;
using Raven.Client.Connection.Async;
using Raven.Json.Linq;
using Raven.Studio.Models;

namespace Raven.Studio.Features.Tasks
{
    public class BackupDatabaseTask : DatabaseTask
    {
        private readonly string backupLocation;

        public BackupDatabaseTask(IAsyncDatabaseCommands databaseCommands, string databaseName, string backupLocation) : base(databaseCommands, "Backup Database", databaseName)
        {
            this.backupLocation = backupLocation;
        }

        protected override async Task<DatabaseTaskOutcome> RunImplementation()
        {
            if (backupLocation == null)
                return DatabaseTaskOutcome.Abandoned;

            var asyncDatabaseCommands = ApplicationModel.Current.Server.Value.DocumentStore
                                                        .AsyncDatabaseCommands
                                                        .ForSystemDatabase();

            string databaseName = ApplicationModel.Database.Value.Name;
            var httpJsonRequest = asyncDatabaseCommands.CreateRequest(
                "/admin/databases/" + databaseName, "GET");

            var doc = await httpJsonRequest.ReadResponseJsonAsync();
            if (doc == null)
            {
                ReportError("Could not find database document for " + databaseName);
                return DatabaseTaskOutcome.Error;
            }

            var databaseDocument = ApplicationModel.Current.Server.Value.DocumentStore.Conventions.CreateSerializer()
                                                   .Deserialize<DatabaseDocument>(new RavenJTokenReader(doc));

            await DatabaseCommands.GlobalAdmin.StartBackupAsync(backupLocation, databaseDocument, databaseName);

            var hasCompleted = false;
            var lastMessageTimeStamp = DateTime.MinValue;

            while (!hasCompleted)
            {
                await TaskEx.Delay(TimeSpan.FromSeconds(1));

                var document =
                    await ApplicationModel.DatabaseCommands.GetAsync(BackupStatus.RavenBackupStatusDocumentKey);

                var documentConvention = ApplicationModel.Current.Server.Value.Conventions;
                var status =
                    documentConvention.CreateSerializer()
                                      .Deserialize<BackupStatus>(new RavenJTokenReader(document.DataAsJson));

                foreach (var backupMessage in status.Messages)
                {
                    if (backupMessage.Timestamp > lastMessageTimeStamp)
                    {
                        Report("[" + backupMessage.Timestamp + "]   	" + backupMessage.Severity + " :    	" +
                               backupMessage.Message);
                        lastMessageTimeStamp = backupMessage.Timestamp;
                    }
                }

                hasCompleted = status.Completed != null;
            }

            return DatabaseTaskOutcome.Succesful;
        }

		public override void OnError()
		{

		}
    }
}
