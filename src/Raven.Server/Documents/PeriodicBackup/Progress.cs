using System;
using Raven.Client.Documents.Operations.Backups;

namespace Raven.Server.Documents.PeriodicBackup
{
    public class Progress
    {
        public UploadProgress UploadProgress { get; set; }

        public Action OnUploadProgress { get; set; } = () => { };
    }
}
