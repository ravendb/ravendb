using System.Collections.Generic;
using Microsoft.AspNetCore.Http;
using Raven.Client.Documents.Commands;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Server.Documents.Sharding.Executors;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Operations
{
    internal readonly struct ShardedGetRevisionsByChangeVectorsOperation : IShardedReadOperation<BlittableArrayResult, BlittableJsonReaderObject[]>
    {
        private readonly HttpContext _httpContext;
        private readonly string[] _changeVectors;
        private readonly bool _metadataOnly;
        private readonly JsonOperationContext _context;

        public ShardedGetRevisionsByChangeVectorsOperation(HttpContext httpContext, string[] changeVectors, bool metadataOnly, JsonOperationContext context, string etag)
        {
            _httpContext = httpContext;
            _changeVectors = changeVectors;
            _metadataOnly = metadataOnly;
            _context = context;
            ExpectedEtag = etag;
        }

        public HttpRequest HttpRequest => _httpContext.Request;

        public string ExpectedEtag { get; }

        public BlittableJsonReaderObject[] CombineResults(Dictionary<int, ShardExecutionResult<BlittableArrayResult>> results)
        {
            int len = 0;
            foreach (var s in results.Values)
            {
                if (s.Result != null)
                {
                    len = s.Result.Results.Length;
                }
            }

            //expecting to get null if single cv is sent and revision not found anywhere
            if (len == 0 && _changeVectors.Length == 1)
                return null;

            var combined = new BlittableJsonReaderObject[len];

            for (int j = 0; j < len; j++)
            {
                foreach (var s in results.Values)
                {
                    if (s.Result == null)
                        continue;

                    if (s.Result.Results[j] != null && s.Result.Results[j] is BlittableJsonReaderObject rev)
                    {
                        combined[j] = rev.Clone(_context);
                        break;
                    }
                }
            }

            return combined;
        }

        public RavenCommand<BlittableArrayResult> CreateCommandForShard(int shardNumber) => new GetRevisionsCommand(_changeVectors, _metadataOnly);
    }
}
