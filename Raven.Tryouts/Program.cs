using System;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.FileSystem;
using Raven.Abstractions.Smuggler;
using Raven.Database.Extensions;
using Raven.Json.Linq;
using Raven.Powershell;
using Raven.Smuggler;
using Raven.Tests.Common;
using Raven.Tests.FileSystem;
using Raven.Tests.Raft.Client;
using Raven.Tests.Smuggler;
using Raven.Tests.Subscriptions;
using Xunit;
#if !DNXCORE50
using Raven.Tests.Sorting;
using Raven.SlowTests.RavenThreadPool;
using Raven.Tests.Core;
using Raven.Tests.Core.Commands;
using Raven.Tests.Issues;
using Raven.Tests.MailingList;
using Raven.Tests.FileSystem.ClientApi;
#endif

namespace Raven.Tryouts
{  
    
    public class Program
    {
        public static void Main(string[] args)
        {
            var backupCmdlet = new DatabaseBackupCmdlet();

            /*
             
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
             */
            backupCmdlet.ServerUrl = "http://localhost:8080";
            backupCmdlet.BackupLocation = "c:\\temp\\reproBackup";
            backupCmdlet.Incremental = true;
            backupCmdlet.DatabaseName = "VoronBackup";
            while (backupCmdlet.Invoke().GetEnumerator().MoveNext());
        }

        public static async Task AsyncMain()
        {
           

            var sp = Stopwatch.StartNew();
            try
            {
                var smugglerApi = new SmugglerFilesApi();
                await smugglerApi.ImportData(importOptions: new SmugglerImportOptions<FilesConnectionStringOptions>()
                {
                    To = new FilesConnectionStringOptions()
                    {
                        DefaultFileSystem = "FS2",
                        Url = "http://localhost:8080",

                    },
                    FromFile = "c:\\Temp\\export.ravendump",
                });
            }
            catch (Exception ex)
            {

                Console.WriteLine(ex);

                Console.WriteLine(ex.StackTrace);
            }

            Console.ReadLine();

            Console.WriteLine(sp.ElapsedMilliseconds);


        }
    }
}
