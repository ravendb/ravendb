using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Extensions;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.Counters
{
    /// <summary>
    /// Represents the result of executing a <see cref="GetCountersOperation"/> or <see cref="CounterBatchOperation"/>, 
    /// containing details of counters associated with a document.
    /// </summary>
    public sealed class CountersDetail
    {
        /// <summary>
        /// Gets or sets the list of counter details retrieved by the operation.
        /// Each <see cref="CounterDetail"/> in the list provides information about a specific counter, 
        /// including its name and value.
        /// </summary>
        /// <remarks>
        /// This property contains the results of either:
        /// - A <see cref="GetCountersOperation"/>: Retrieves details of counters for a document.
        /// - A <see cref="CounterBatchOperation"/>: Returns details of counters affected by a batch operation.
        /// </remarks>
        public List<CounterDetail> Counters { get; set; }

        public CountersDetail()
        {
            Counters = new List<CounterDetail>();
        }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Counters)] = new DynamicJsonArray(Counters.Select(x => x?.ToJson()))
            };
        }
    }

    /// <summary>
    /// Represents detailed information about a counter associated with a document in RavenDB.
    /// </summary>
    public sealed class CounterDetail
    {
        /// <summary>
        /// Gets or sets the ID of the document to which the counter belongs.
        /// </summary>
        public string DocumentId { get; set; }

        /// <summary>
        /// Gets or sets the name of the counter.
        /// </summary>
        /// <remarks>
        /// This identifies the specific counter within the document.
        /// </remarks>
        public string CounterName { get; set; }

        /// <summary>
        /// Gets or sets the total value of the counter across all nodes.
        /// </summary>
        /// <remarks>
        /// This value is the aggregate of all per-node counter values stored in <see cref="CounterValues"/>.
        /// </remarks>
        public long TotalValue { get; set; }

        /// <summary>
        /// Gets or sets the ETag associated with the counter.
        /// </summary>
        /// <remarks>
        /// The ETag is a unique identifier used to track changes to the counter.
        /// </remarks>
        public long Etag { get; set; }

        /// <summary>
        /// Gets or sets the dictionary of counter values for each node in the cluster.
        /// </summary>
        /// <remarks>
        /// The key is the node identifier, and the value is the counter value for that node.
        /// </remarks>
        public Dictionary<string, long> CounterValues { get; set; }

        /// <summary>
        /// Gets or sets the change vector for the counter.
        /// </summary>
        /// <remarks>
        /// The change vector represents the version history of the counter and is used for concurrency control.
        /// </remarks>
        public string ChangeVector { get; set; }

        internal LazyStringValue CounterKey { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(DocumentId)] = DocumentId,
                [nameof(CounterName)] = CounterName,
                [nameof(TotalValue)] = TotalValue,
                [nameof(CounterValues)] = CounterValues?.ToJson()
            };
        }
    }

    /// <summary>
    /// Represents detailed information about a group of counters associated with a document in RavenDB.
    /// </summary>
    public sealed class CounterGroupDetail : IDisposable
    {
        /// <summary>
        /// Gets or sets the ID of the document to which the counter group belongs.
        /// </summary>
        /// <remarks>
        /// This property identifies the document that holds the counters in this group.
        /// </remarks>
        public LazyStringValue DocumentId { get; set; }

        /// <summary>
        /// Gets or sets the change vector for the counter group.
        /// </summary>
        /// <remarks>
        /// The change vector represents the version history of the counter group and is used for concurrency control.
        /// </remarks>
        public LazyStringValue ChangeVector { get; set; }

        /// <summary>
        /// Gets or sets the raw Blittable JSON object containing the counter values in the group.
        /// </summary>
        public BlittableJsonReaderObject Values { get; set; }

        /// <summary>
        /// Gets or sets the ETag for the counter group.
        /// </summary>
        /// <remarks>
        /// The ETag is a unique identifier used to track changes to the counter group.
        /// </remarks>
        public long Etag { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(DocumentId)] = DocumentId,
                [nameof(ChangeVector)] = ChangeVector,
                [nameof(Values)] = Values,
                [nameof(Etag)] = Etag
            };
        }

        public void Dispose()
        {
            DocumentId?.Dispose();
            ChangeVector?.Dispose();
            Values?.Dispose();
        }
    }
}
