using System;
using Raven.Client.Documents.Commands;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Server.Documents.Handlers.Processors.Revisions;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Operations
{
    internal readonly struct ShardedGetRevisionsByChangeVectorsOperation : IShardedOperation<BlittableArrayResult, RevisionsResult>
    {
        private readonly string[] _changeVectors;
        private readonly bool _metadataOnly;
        private readonly JsonOperationContext _context;

        public ShardedGetRevisionsByChangeVectorsOperation(string[] changeVectors, bool metadataOnly, JsonOperationContext context)
        {
            _changeVectors = changeVectors;
            _metadataOnly = metadataOnly;
            _context = context;
        }

        public RevisionsResult Combine(Memory<BlittableArrayResult> results)
        {
            var span = results.Span;

            int len = 0;
            long total = 0;
            foreach (var s in span)
            {
                if (s != null)
                {
                    len = s.Results.Length;
                    total += s.TotalResults;
                }
            }

            //expecting to get null if single cv is sent and revision not found anywhere
            if (total == 0 && _changeVectors.Length == 1)
                return null;

            var combined = new RevisionsResult()
            {
                Results = new BlittableJsonReaderObject[len],
                TotalResults = total
            };

            for (int j = 0; j < len; j++)
            {
                foreach (var s in span)
                {
                    if (s == null)
                        continue;

                    if (s.Results[j] != null && s.Results[j] is BlittableJsonReaderObject rev)
                    {
                        combined.Results[j] = rev.Clone(_context);
                        break;
                    }
                }
            }

            return combined;
        }

        public RavenCommand<BlittableArrayResult> CreateCommandForShard(int shard) => new GetRevisionsCommand(_changeVectors, _metadataOnly);
    }
}
