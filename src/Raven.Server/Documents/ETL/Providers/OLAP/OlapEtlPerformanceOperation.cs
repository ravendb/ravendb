using System;
using Raven.Client.Documents.Operations.Backups;
using Raven.Server.Documents.ETL.Stats;

namespace Raven.Server.Documents.ETL.Providers.OLAP
{
    public class OlapEtlPerformanceOperation : EtlPerformanceOperation
    {
        public OlapEtlPerformanceOperation(TimeSpan duration)
            : base(duration)
        {
        }

        public UploadProgress AzureUpload { get; set; }
        public UploadProgress FtpUpload { get; set; }
        public UploadProgress GlacierUpload { get; set; }
        public UploadProgress GoogleCloudUpload { get; set; }
        public UploadProgress S3Upload { get; set; }
        public int NumberOfFiles { get; set; }
        public string FileName { get; set; }
    }
}
