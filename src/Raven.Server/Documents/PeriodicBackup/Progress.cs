using System;
using Raven.Client.Documents.Operations.Backups;

namespace Raven.Server.Documents.PeriodicBackup
{
    public class Progress
    {
        public Progress(UploadProgress progress = null)
        {
            UploadProgress = progress ?? new UploadProgress();
        }

        public UploadProgress UploadProgress { get; }

        public Action OnUploadProgress { get; set; } = () => { };
    }
}
