using System.Collections.Generic;
using System.Linq;
using Raven.Client.Extensions;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.Counters
{
    public class CountersDetail
    {
        public List<CounterDetail> Counters { get; set; }

        public CountersDetail()
        {
            Counters = new List<CounterDetail>();
        }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Counters)] = new DynamicJsonArray(Counters.Select(x => x.ToJson()))
            };
        }
    }

    public class CounterDetail
    {
        public string DocumentId { get; set; }
        internal LazyStringValue LazyDocumentId { get; set; }
        public string CounterName { get; set; }
        public long TotalValue { get; set; }
        public long Etag { get; set; }
        public Dictionary<string, long> CounterValues { get; set; }

        public string ChangeVector { get; set; }

        internal string DbId { get; set; }

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
}
