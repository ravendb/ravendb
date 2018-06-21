using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session;
using Sparrow.Json;

namespace Raven.Client.Documents.Queries.Timings
{
    public class QueryTimings : IFillFromBlittableJson
    {
        public long DurationInMs { get; set; }

        public Dictionary<string, QueryTimings> Timings { get; set; }

        public void FillFromBlittableJson(BlittableJsonReaderObject json)
        {
            var timings = (QueryTimings)EntityToBlittable.ConvertToEntity(typeof(QueryTimings), "query/timings", json, DocumentConventions.Default);
            DurationInMs = timings.DurationInMs;
            Timings = timings.Timings;
        }

        internal QueryTimings Clone()
        {
            return new QueryTimings
            {
                DurationInMs = DurationInMs,
                Timings = Timings?.ToDictionary(x => x.Key, x => x.Value.Clone())
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
