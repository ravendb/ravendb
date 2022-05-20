//-----------------------------------------------------------------------
// <copyright file="DatabaseStatistics.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Linq;
using Raven.Client.Util;

namespace Raven.Client.Documents.Operations
{
    public class DatabaseStatistics : AbstractDatabaseStatistics<IndexInformation>
    {
        /// <summary>
        /// Last document etag in database.
        /// </summary>
        public long? LastDocEtag { get; set; }

        /// <summary>
        /// Last database etag
        /// </summary>
        public long? LastDatabaseEtag { get; set; }

        /// <summary>
        /// Total number of attachments in database.
        /// </summary>
        public long CountOfUniqueAttachments { get; set; }

        /// <summary>
        /// List of stale index names in database..
        /// </summary>
        public string[] StaleIndexes => Indexes?.Where(x => x.IsStale).Select(x => x.Name).ToArray();

        /// <summary>
        /// Global change vector of the database.
        /// </summary>
        public string DatabaseChangeVector { get; set; }

        /// <summary>
        /// Database identifier.
        /// </summary>
        public string DatabaseId { get; set; }

        /// <summary>
        /// Indicates if process is 64-bit
        /// </summary>
        public bool Is64Bit { get; set; }

        public string Pager { get; set; }

        public DateTime? LastIndexingTime { get; set; }

        public Size SizeOnDisk { get; set; }

        public Size TempBuffersSizeOnDisk { get; set; }

        public int NumberOfTransactionMergerQueueOperations { get; set; }
    }
}
