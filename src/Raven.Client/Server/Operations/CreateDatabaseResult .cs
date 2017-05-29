//-----------------------------------------------------------------------
// <copyright file="PutResult.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System.Collections.Generic;

namespace Raven.Client.Server.Operations
{
    /// <summary>
    /// The result of a create database operation
    /// </summary>
    public class CreateDatabaseResult
    {
        /// <summary>
        /// Key of the database .
        /// </summary>
        public string Key { get; set; }

        /// <summary>
        /// long? of the database after PUT operation.
        /// </summary>
        public long? ETag { get; set; }

        public DatabaseTopology Topology { get; set; }

        public List<string> NodesAddedTo { get; set; }

    }

    public class ModifyDatabaseWatchersResult : CreateDatabaseResult
    {
    }
}
