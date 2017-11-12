using System;
using System.Linq;
using System.Management.Automation;
using Raven.Abstractions.Data;
using Raven.Client.Connection;
using Raven.Client.Document;
using Raven.Json.Linq;

namespace Raven.Powershell
{
    [Cmdlet(VerbsData.Backup, "Database")]
    public class DatabaseBackupCmdlet : Cmdlet
    {
        private bool incremental;

        [Parameter(ValueFromPipelineByPropertyName = true, Mandatory = true, HelpMessage = "Url of RavenDB server, including the port. Example --> http://localhost:8080")]
        public string ServerUrl { get; set; }

        [Parameter(ValueFromPipelineByPropertyName = true, Mandatory = true, HelpMessage = "Database name in RavenDB server")]
        public string DatabaseName { get; set; }

        [Parameter(ValueFromPipelineByPropertyName = true, HelpMessage = "ApiKey to use when connecting to RavenDB Server. It should be full API key. Example --> key1/sAdVA0KLqigQu67Dxj7a")]
        public string ApiKey { get; set; }       

        [Parameter(ValueFromPipelineByPropertyName = true, HelpMessage = "If true, incremental backup. Otherwise, do a full backup.")]
        public SwitchParameter Incremental
        {
            get { return incremental; }
            set { incremental = value; }
        }

        [Parameter(ValueFromPipelineByPropertyName = true, Mandatory = true, HelpMessage = "Where to write the backup")]
        public string BackupLocation { get; set; }

        protected override void ProcessRecord()
        {
            using (var store = new DocumentStore
            {
                Url = ServerUrl,
                ApiKey = ApiKey,
                DefaultDatabase = DatabaseName
            })
            {
                store.Initialize();
                var databaseNames = store.DatabaseCommands.GlobalAdmin.GetDatabaseNames(int.MaxValue);
                if (databaseNames.Contains(DatabaseName) == false)
                {
                    WriteError(new ErrorRecord(new InvalidOperationException("Database with name = " + DatabaseName + " does not exist..."), "NoDatabase",ErrorCategory.ObjectNotFound,store));
                    return;
                }

                var databaseId = "Raven/Databases/" + DatabaseName;

                var databaseDocuments = store.DatabaseCommands.ForSystemDatabase().Get(databaseId).DataAsJson.Deserialize(typeof(DatabaseDocument), store.Conventions) as DatabaseDocument;
                
                var operation = store.DatabaseCommands.GlobalAdmin.StartBackup(BackupLocation, null, incremental, DatabaseName);
                operation.OnProgressChanged += progress =>
                {
                    WriteProgress(new ProgressRecord(0,"Database Backup", "Backing up the database.")
                    {
                        PercentComplete = progress.ProcessedEntries / progress.TotalEntries * 100
                    });
                };

                operation.WaitForCompletion();
            }
        }
    }
}
