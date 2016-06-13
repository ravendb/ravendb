//-----------------------------------------------------------------------
// <copyright file="PeriodicExportConfiguration.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

namespace Raven.Server.Documents.PeriodicExport
{
    public class PeriodicExportConfiguration
    {
        /// <summary>
        /// Indicates if periodic export is active.
        /// </summary>
        public bool Active { get; set; }

        /// <summary>
        /// Amazon Glacier Vaul name.
        /// </summary>
        public string GlacierVaultName { get; set; }

        /// <summary>
        /// Amazon S3 Bucket name.
        /// </summary>
        public string S3BucketName { get; set; }

        /// <summary>
        /// Amazon Web Services (AWS) region.
        /// </summary>
        public string AwsRegionEndpoint { get; set; }

        /// <summary>
        /// Microsoft Azure Storage Container name.
        /// </summary>
        public string AzureStorageContainer { get; set; }

        /// <summary>
        /// Path to local folder. If not empty, exports will be held in this folder and not deleted. Otherwise, exports will be created in DataDir of a database and deleted after successful upload to Glacier/S3/Azure.
        /// </summary>
        public string LocalFolderName { get; set; }

        /// <summary>
        /// Path to remote azure folder.
        /// </summary>
        public string AzureRemoteFolderName { get; set; }

        /// <summary>
        /// Path to remote azure folder.
        /// </summary>
        public string S3RemoteFolderName { get; set; }

        /// <summary>
        /// Interval between incremental exports in milliseconds. If set to null or 0 then incremental periodic export will be disabled.
        /// </summary>
        public long? IntervalMilliseconds { get; set; }

        /// <summary>
        /// Interval between full exports in milliseconds. If set to null or 0 then full periodic export will be disabled.
        /// </summary>
        public long? FullExportIntervalMilliseconds { get; set; }
    }
}