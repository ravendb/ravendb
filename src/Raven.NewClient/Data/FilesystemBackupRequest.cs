//-----------------------------------------------------------------------
// <copyright file="DatabaseBackupRequest.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using Raven.NewClient.Abstractions.FileSystem;

namespace Raven.NewClient.Abstractions.Data
{
    public class FilesystemBackupRequest
    {
        /// <summary>
        /// Path to directory where backup should lie (must be accessible from server).
        /// </summary>
        public string BackupLocation { get; set; }

        /// <summary>
        /// FileSystemDocument that will be inserted with backup. If null then document will be taken from server.
        /// </summary>
        public FileSystemDocument FileSystemDocument { get; set; }
    }
}
