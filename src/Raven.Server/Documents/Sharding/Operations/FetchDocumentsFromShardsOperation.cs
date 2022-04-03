using System;
using System.Collections.Generic;
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
        private readonly string _etag;
        private readonly string[] _includePaths;
        private readonly bool _metadataOnly;

        public FetchDocumentsFromShardsOperation(
            TransactionOperationContext context,
            ShardedDocumentHandler handler,
            Dictionary<int, ShardLocator.IdsByShard<string>> idsByShards,
            string etag,
            string[] includePaths,
            bool metadataOnly)
        {
            _context = context;
            _handler = handler;
            _databaseContext = handler.DatabaseContext;
            _idsByShards = idsByShards;
            _etag = etag;
            _includePaths = includePaths;
            _metadataOnly = metadataOnly;
        }

        public string ExpectedEtag => _etag;

        public GetShardedDocumentsResult CombineResults(Memory<GetDocumentsResult> results)
        {
            var span = results.Span;
            var docs = new List<BlittableJsonReaderObject>();
            var includesMap = new Dictionary<string, BlittableJsonReaderObject>(StringComparer.OrdinalIgnoreCase);
            var missingIncludes = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            foreach (var cmd in span)
            {
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

                foreach (BlittableJsonReaderObject cmdResult in cmdResults)
                {
                    if (_includePaths != null)
                    {
                        foreach (string includePath in _includePaths)
                        {
                            IncludeUtil.GetDocIdFromInclude(cmdResult, includePath, missingIncludes, _databaseContext.IdentityPartsSeparator);
                        }
                    }
                    docs.Add(cmdResult.Clone(_context));
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
        public List<BlittableJsonReaderObject> Results;
        public BlittableJsonReaderObject[] CounterIncludes;
        public BlittableJsonReaderObject[] RevisionIncludes;
        public BlittableJsonReaderObject[] TimeSeriesIncludes;
        public BlittableJsonReaderObject[] CompareExchangeValueIncludes;

        public HashSet<string> MissingIncludes;
    }
}
