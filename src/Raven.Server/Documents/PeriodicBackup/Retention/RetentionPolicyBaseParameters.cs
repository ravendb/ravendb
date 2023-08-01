using System;
using System.Threading;
using Raven.Client.Documents.Operations.Backups;

namespace Raven.Server.Documents.PeriodicBackup.Retention
{
    public sealed class RetentionPolicyBaseParameters
    {
        public RetentionPolicy RetentionPolicy { get; set; }

        public string DatabaseName { get; set; }

        public bool IsFullBackup { get; set; }

        public Action<string> OnProgress { get; set; }

        public CancellationToken CancellationToken { get; set; }
    }
}
