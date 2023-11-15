using System;
using System.Diagnostics;
using Raven.Client.Documents.Operations.Backups;
using Raven.Server.Utils.Metrics;
using Sparrow;

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

        public static Progress Get(UploadProgress uploadProgress, Action<string> onProgress)
        {
            var bytesPutsPerSec = new MeterMetric();

            long lastUploadedInBytes = 0;
            var sw = Stopwatch.StartNew();
            var progress = new Progress(uploadProgress)
            {
                OnUploadProgress = () =>
                {
                    if (sw.ElapsedMilliseconds <= 2000)
                        return;

                    var totalUploadedInBytes = uploadProgress.UploadedInBytes;
                    bytesPutsPerSec.MarkSingleThreaded(totalUploadedInBytes - lastUploadedInBytes);
                    lastUploadedInBytes = totalUploadedInBytes;
                    var uploaded = new Size(totalUploadedInBytes, SizeUnit.Bytes);
                    var totalToUpload = new Size(uploadProgress.TotalInBytes, SizeUnit.Bytes);
                    uploadProgress.BytesPutsPerSec = bytesPutsPerSec.MeanRate;
                    onProgress($"Uploaded: {uploaded} / {totalToUpload}");
                    sw.Restart();
                }
            };

            return progress;
        }
    }
}
