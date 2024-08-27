using System;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.Documents.Smuggler;
using Sparrow.Logging;
using Voron.Recovery;
using Xunit.Abstractions;

namespace SlowTests.RecoveryTests
{
    public abstract class RecoveryTestBase : RavenTestBase
    {
        protected RecoveryTestBase(ITestOutputHelper output) : base(output)
        {
        }

        [Flags]
        public enum RecoveryTypes
        {
            Documents = 1,
            Revisions = 2,
            Conflicts = 4,
            Counters = 8,
            TimeSeries = 16,

            All = Documents | Revisions | Conflicts | Counters | TimeSeries
        }

        public class RecoveryOptions
        {
            [NotNull] public string PathToDataFile;
            [NotNull] public string RecoveryDirectory;
            public RecoveryTypes RecoveryTypes = RecoveryTypes.All;
            public bool DisableCopyOnWriteMode;

            public byte[] MasterKey;
            public X509Certificate2 AdminCertificate;
            public X509Certificate2 ClientCertificate;
        }

        public async Task<DocumentStore> RecoverDatabase(RecoveryOptions options)
        {
            var recoveryExportPath = options.RecoveryDirectory;
            using (var recovery = new Recovery(new VoronRecoveryConfiguration
            {
                LoggingLevel = LogLevel.Off,
                DataFileDirectory = options.PathToDataFile,
                PathToDataFile = Path.Combine(options.PathToDataFile, "Raven.voron"),
                OutputFileName = Path.Combine(recoveryExportPath, "recovery.ravendbdump"),
                MasterKey = options.MasterKey,
                DisableCopyOnWriteMode = options.DisableCopyOnWriteMode
            }))
            {
                recovery.Execute(Console.Out, CancellationToken.None);
            }

            var store = GetDocumentStore(new Options { AdminCertificate = options.AdminCertificate, ClientCertificate = options.ClientCertificate });

            if (options.RecoveryTypes.HasFlag(RecoveryTypes.Documents))
                await ImportFile(store, recoveryExportPath, "recovery-2-Documents.ravendbdump");

            if (options.RecoveryTypes.HasFlag(RecoveryTypes.Revisions))
            {
                await store.Maintenance.SendAsync(new ConfigureRevisionsOperation(new RevisionsConfiguration { Default = new RevisionsCollectionConfiguration { } }));
                await ImportFile(store, recoveryExportPath, "recovery-3-Revisions.ravendbdump");
            }

            if (options.RecoveryTypes.HasFlag(RecoveryTypes.Conflicts))
                await ImportFile(store, recoveryExportPath, "recovery-4-Conflicts.ravendbdump");

            if (options.RecoveryTypes.HasFlag(RecoveryTypes.Counters))
                await ImportFile(store, recoveryExportPath, "recovery-5-Counters.ravendbdump");

            if (options.RecoveryTypes.HasFlag(RecoveryTypes.TimeSeries))
                await ImportFile(store, recoveryExportPath, "recovery-6-TimeSeries.ravendbdump");

            return store;
        }

        private static async Task ImportFile(DocumentStore store, string rootPath, string file)
        {
            var path = Path.Combine(rootPath, file);
            if (File.Exists(path) == false)
                return;

            var op = await store.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), path);
            await op.WaitForCompletionAsync(TimeSpan.FromMinutes(2));
        }
    }
}
