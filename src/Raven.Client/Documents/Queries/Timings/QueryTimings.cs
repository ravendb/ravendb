using System.Collections.Generic;
using Raven.Client.Documents.Conventions;
using Sparrow.Json;

namespace Raven.Client.Documents.Queries.Timings
{
    public class QueryTimings : IFillFromBlittableJson
    {
        public long DurationInMs { get; set; }

        public IDictionary<string, QueryTimings> Timings { get; set; }

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
    }
}
