//-----------------------------------------------------------------------
// <copyright file="DatabaseBackupRequest.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
namespace Raven.NewClient.Abstractions.Data
{
    public class DatabaseBackupRequest
    {
        /// <summary>
        /// Path to directory where backup should lie (must be accessible from server).
        /// </summary>
        public string BackupLocation { get; set; }

        /// <summary>
        /// DatabaseDocument that will be inserted with backup. If null then document will be taken from server.
        /// </summary>
        public DatabaseDocument DatabaseDocument { get; set; }
    }
}
