//-----------------------------------------------------------------------
// <copyright file="DatabaseStatistics.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Linq;
using Raven.Client.Documents.Indexes;
using Raven.Client.Util;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations
{
    public class DetailedDatabaseStatistics : DatabaseStatistics, IDynamicJson
    {
        /// <summary>
        /// Total number of identities in database.
        /// </summary>
        public long CountOfIdentities { get; set; }

        /// <summary>
        /// Total number of compare-exchange values in database.
        /// </summary>
        public long CountOfCompareExchange { get; set; }

        /// <summary>
        /// Total number of compare-exchange tombstones values in database.
        /// </summary>
        public long CountOfCompareExchangeTombstones { get; set; }

        public DynamicJsonValue ToJson()
        {
            var json = base.ToJson();
            json[nameof(CountOfIdentities)] = CountOfIdentities;
            json[nameof(CountOfCompareExchange)] = CountOfCompareExchange;
            json[nameof(CountOfCompareExchangeTombstones)] = CountOfCompareExchangeTombstones;
            return json;
        }
    }

    public class DatabaseStatistics : IDynamicJson
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
        /// Total number of indexes in database.
        /// </summary>
        public int CountOfIndexes { get; set; }

        /// <summary>
        /// Total number of documents in database.
        /// </summary>
        public long CountOfDocuments { get; set; }

        /// <summary>
        /// Total number of revision documents in database.
        /// </summary>
        public long CountOfRevisionDocuments { get; set; }

        /// <summary>
        /// Total number of documents conflicts in database.
        /// </summary>
        public long CountOfDocumentsConflicts { get; set; }

        /// <summary>
        /// Total number of tombstones in database.
        /// </summary>
        public long CountOfTombstones { get; set; }

        /// <summary>
        /// Total number of conflicts in database.
        /// </summary>
        public long CountOfConflicts { get; set; }

        /// <summary>
        /// Total number of attachments in database.
        /// </summary>
        public long CountOfAttachments { get; set; }

        /// <summary>
        /// Total number of attachments in database.
        /// </summary>
        public long CountOfUniqueAttachments { get; set; }

        /// <summary>
        /// Total number of counter-group entries in database.
        /// </summary>
        public long CountOfCounterEntries { get; set; }

        /// <summary>
        /// List of stale index names in database..
        /// </summary>
        public string[] StaleIndexes => Indexes?.Where(x => x.IsStale).Select(x => x.Name).ToArray();

        /// <summary>
        /// Statistics for each index in database.
        /// </summary>
        public IndexInformation[] Indexes { get; set; }

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
        
        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(LastDocEtag)] = LastDocEtag,
                [nameof(LastDatabaseEtag)] = LastDatabaseEtag,
                [nameof(CountOfIndexes)] = CountOfIndexes,
                [nameof(CountOfDocuments)] = CountOfDocuments,
                [nameof(CountOfRevisionDocuments)] = CountOfRevisionDocuments,
                [nameof(CountOfDocumentsConflicts)] = CountOfDocumentsConflicts,
                [nameof(CountOfTombstones)] = CountOfTombstones,
                [nameof(CountOfConflicts)] = CountOfConflicts,
                [nameof(CountOfAttachments)] = CountOfAttachments,
                [nameof(CountOfUniqueAttachments)] = CountOfUniqueAttachments,
                [nameof(CountOfCounterEntries)] = CountOfCounterEntries,
                [nameof(StaleIndexes)] = new DynamicJsonArray(StaleIndexes),
                [nameof(Indexes)] = Indexes,
                [nameof(DatabaseChangeVector)] = DatabaseChangeVector,
                [nameof(DatabaseId)] = DatabaseId,
                [nameof(Is64Bit)] = Is64Bit,
                [nameof(Pager)] = Pager,
                [nameof(LastIndexingTime)] = LastIndexingTime,
                [nameof(SizeOnDisk)] = SizeOnDisk.ToJson(),
                [nameof(TempBuffersSizeOnDisk)] = TempBuffersSizeOnDisk.ToJson(),
                [nameof(NumberOfTransactionMergerQueueOperations)] = NumberOfTransactionMergerQueueOperations,
            }; 
        }
    }

    public class IndexInformation : IDynamicJson
    {

        public string Name { get; set; }

        public bool IsStale { get; set; }

        public IndexState State { get; set; }

        public IndexLockMode LockMode { get; set; }

        public IndexPriority Priority { get; set; }

        public IndexType Type { get; set; }

        public DateTime? LastIndexingTime { get; set; }
        
        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Name)] = Name,
                [nameof(IsStale)] = IsStale,
                [nameof(State)] = State,
                [nameof(LockMode)] = LockMode,
                [nameof(Priority)] = Priority,
                [nameof(Priority)] = Priority,
                [nameof(LastIndexingTime)] = LastIndexingTime
            };
        }
    }
}
