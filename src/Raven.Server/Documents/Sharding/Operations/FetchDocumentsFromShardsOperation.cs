using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.AspNetCore.Http;
using Raven.Client.Documents.Commands;
using Raven.Client.Http;
using Raven.Server.Documents.Sharding.Handlers;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Utils;

namespace Raven.Server.Documents.Sharding.Operations
{
    public readonly struct FetchDocumentsFromShardsOperation : IShardedReadOperation<GetDocumentsResult, GetShardedDocumentsResult>
    {
        private readonly TransactionOperationContext _context;
        private readonly ShardedDocumentHandler _handler;
        private readonly ShardedDatabaseContext _databaseContext;
        private readonly Dictionary<int, ShardLocator.IdsByShard<string>> _idsByShards;
        private readonly int _size;
        private readonly string _etag;
        private readonly string[] _includePaths;
        private readonly bool _metadataOnly;


        public FetchDocumentsFromShardsOperation(
            TransactionOperationContext context,
            ShardedDocumentHandler handler,
            Dictionary<int, ShardLocator.IdsByShard<string>> idsByShards,
            int size,
            string etag,
            string[] includePaths,
            bool metadataOnly)
        {
            _context = context;
            _handler = handler;
            _databaseContext = handler.DatabaseContext;
            _idsByShards = idsByShards;
            _size = size;
            _etag = etag;
            _includePaths = includePaths;
            _metadataOnly = metadataOnly;
        }

        public string ExpectedEtag => _etag;

        public GetShardedDocumentsResult CombineResults(Memory<GetDocumentsResult> results)
        {
            var span = results.Span;
            var docs = new BlittableJsonReaderObject[_size];
            var includesMap = new Dictionary<string, BlittableJsonReaderObject>(StringComparer.OrdinalIgnoreCase);
            var missingIncludes = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            for (var shardNumber = 0; shardNumber < span.Length; shardNumber++)
            {
                var cmd = span[shardNumber];
                if (cmd == null)
                    continue;

                var cmdResults = cmd.Results;

                var cmdIncludes = cmd.Includes;
                if (cmdIncludes != null)
                {
                    BlittableJsonReaderObject.PropertyDetails prop = default;
                    for (int i = 0; i < cmdIncludes.Count; i++)
                    {
                        cmdIncludes.GetPropertyByIndex(i, ref prop);
                        includesMap[prop.Name] = ((BlittableJsonReaderObject)prop.Value).Clone(_context);
                    }
                }

                var positions = _idsByShards.ElementAt(shardNumber).Value.Positions;
                for (var i = 0; i < positions.Count; i++)
                {
                    var position = positions[i];
                    var doc = (BlittableJsonReaderObject)cmdResults[i];
                    if (_includePaths != null)
                    {
                        foreach (string includePath in _includePaths)
                        {
                            IncludeUtil.GetDocIdFromInclude(doc, includePath, missingIncludes, _databaseContext.IdentityPartsSeparator);
                        }
                    }

                    docs[position] = doc.Clone(_context);
                }
            }

            foreach (var kvp in includesMap) // remove the items we already have
            {
                missingIncludes.Remove(kvp.Key);
            }

            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Karmel, DevelopmentHelper.Severity.Normal, "Handle include of counters/time-series/compare exchange..");

            return new GetShardedDocumentsResult
            {
                Results = docs,
                Includes = includesMap,
                MissingIncludes = missingIncludes
            };
        }

        public HttpRequest HttpRequest => _handler.HttpContext.Request;
        public RavenCommand<GetDocumentsResult> CreateCommandForShard(int shard) => new GetDocumentsCommand(_idsByShards[shard].Ids.ToArray(), _includePaths, _metadataOnly);
    }

    public class GetShardedDocumentsResult
    {
        public Dictionary<string, BlittableJsonReaderObject> Includes;
        public BlittableJsonReaderObject[] Results;
        public BlittableJsonReaderObject[] CounterIncludes;
        public BlittableJsonReaderObject[] RevisionIncludes;
        public BlittableJsonReaderObject[] TimeSeriesIncludes;
        public BlittableJsonReaderObject[] CompareExchangeValueIncludes;

        public HashSet<string> MissingIncludes;
    }
}
