using System.Collections.Generic;
using Newtonsoft.Json.Linq;
using Raven.Client.Documents.Conventions;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Queries.Timings
{
    public sealed class QueryTimings : IFillFromBlittableJson, IDynamicJson
    {
        public long DurationInMs { get; set; }

        public IDictionary<string, QueryTimings> Timings { get; set; }
        
        public object QueryPlan { get; set; }

        internal bool ShouldBeIncluded { get; set; }

        public void FillFromBlittableJson(BlittableJsonReaderObject json)
        {
            var timings = DocumentConventions.Default.Serialization.DefaultConverter.FromBlittable<QueryTimings>(json, "query/timings");
            DurationInMs = timings.DurationInMs;
            Timings = timings.Timings;
        }

        internal QueryTimings Clone()
        {
            SortedDictionary<string, QueryTimings> timings = null;
            if (Timings != null)
            {
                timings = new SortedDictionary<string, QueryTimings>();
                foreach (var kvp in Timings)
                {
                    timings[kvp.Key] = kvp.Value.Clone();
                }
            }

            return new QueryTimings
            {
                DurationInMs = DurationInMs,
                Timings = timings
            };
        }

        internal void Update(QueryResult queryResult)
        {
            DurationInMs = 0;
            Timings = null;

            if (queryResult.Timings == null)
                return;

            DurationInMs = queryResult.Timings.DurationInMs;
            Timings = queryResult.Timings.Timings;
        }

        public DynamicJsonValue ToJson()
        {
            DynamicJsonValue djv = new DynamicJsonValue
            {
                [nameof(DurationInMs)] = DurationInMs,
                [nameof(Timings)] = InnerToJson(Timings)
            };

            return djv;

            DynamicJsonValue InnerToJson(IDictionary<string, QueryTimings> queryTimings)
            {
                DynamicJsonValue json = new DynamicJsonValue();
                if (queryTimings == null)
                    return null;
                foreach (var kvp in queryTimings)
                {
                    DynamicJsonValue innerJson = new DynamicJsonValue()
                    {
                        [nameof(DurationInMs)] = kvp.Value.DurationInMs
                    };
                    innerJson[nameof(Timings)] = InnerToJson(kvp.Value.Timings);
                    json[kvp.Key] = innerJson;
                }

                return json;
            }
        }
    }
}
