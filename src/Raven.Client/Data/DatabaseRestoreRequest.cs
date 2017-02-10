//-----------------------------------------------------------------------
// <copyright file="DatabaseRestoreRequest.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
namespace Raven.NewClient.Abstractions.Data
{
    public class DatabaseRestoreRequest : AbstractRestoreRequest
    {
        /// <summary>
        /// Indicates what should be the name of database after restore. If null then name will be read from 'Database.Document' found in backup.
        /// </summary>
        public string DatabaseName { get; set; }

        /// <summary>
        /// Path to the directory of a new database. If null then default location will be assumed.
        /// </summary>
        public string DatabaseLocation { get; set; }

        /// <summary>
        /// Indicates if all replication destinations should disabled after restore (only when Replication bundle is activated).
        /// </summary>
        public bool DisableReplicationDestinations { get; set; }

        /// <summary>
        /// Indicates if restored database should have new Id generated. By default it will be the same.
        /// </summary>
        public bool GenerateNewDatabaseId { get; set; }
    }
}
