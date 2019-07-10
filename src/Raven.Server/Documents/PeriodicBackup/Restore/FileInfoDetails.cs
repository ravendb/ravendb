using System;
using System.Collections.Generic;

namespace Raven.Server.Documents.PeriodicBackup.Restore
{
    public class ListObjectsResult
    {
        public List<S3FileInfoDetails> FileInfoDetails { get; set; }

        public string ContinuationToken { get; set; }
    }

    public class FileInfoDetails
    {
        public string FullPath { get; set; }

        public DateTime LastModified { get; set; }
    }

    public class S3FileInfoDetails
    {
        public string FullPath { get; set; }

        public string LastModifiedAsString { get; set; }
    }
}
