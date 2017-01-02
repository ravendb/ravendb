using System;
using System.Collections.Generic;
using System.Linq;
using Raven.NewClient.Abstractions.Data;
using Raven.NewClient.Client.Commands;
using Raven.NewClient.Client.Data;
using Raven.NewClient.Client.Data.Queries;
using Raven.NewClient.Client.Document.Batches;
using Raven.NewClient.Client.Shard;
using Sparrow.Json;


namespace Raven.NewClient.Client.Commands.Lazy
{
    public class LazyTransformerLoadOperation<T> : ILazyOperation
    {
        private readonly string[] ids;
        private readonly string transformer;

        private readonly Dictionary<string, object> transformerParameters;

        private readonly LoadTransformerOperation loadTransformerOperation;
        private readonly bool singleResult;

        public LazyTransformerLoadOperation(string[] ids, string transformer, Dictionary<string, object> transformerParameters, LoadTransformerOperation loadTransformerOperation, bool singleResult)
        {
            this.ids = ids;
            this.transformer = transformer;
            this.transformerParameters = transformerParameters;
            this.loadTransformerOperation = loadTransformerOperation;
            this.singleResult = singleResult;
        }

        public GetRequest CreateRequest()
        {
            string query = "?" + string.Join("&", ids.Select(x => "id=" + Uri.EscapeDataString(x)).ToArray());
            if (string.IsNullOrEmpty(transformer) == false)
            {
                query += "&transformer=" + transformer;

                if (transformerParameters != null)
                    query = transformerParameters.Aggregate(query, (current, queryInput) => current + ("&" + string.Format("tp-{0}={1}", queryInput.Key, queryInput.Value)));
            }

            return new GetRequest
            {
                Url = "/docs",
                Query = query
            };
        }

        public object Result { get; set; }

        public QueryResult QueryResult { get; set; }

        public bool RequiresRetry { get; set; }

        public void HandleResponse(BlittableJsonReaderObject response)
        {
            throw new NotImplementedException();
            /*if (response.RequestHasErrors())
            {
                throw new InvalidOperationException("Got bad status code: " + response.Status);
            }

            HandleRespose(new LoadResult
            {
                Includes = response.Result.Value<RavenJArray>("Includes").Cast<RavenJObject>().ToList(),
                Results = response.Result.Value<RavenJArray>("Results").Select(x => x as RavenJObject).ToList()
            });*/
        }
    }
}
