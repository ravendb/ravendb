using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Primitives;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Operations.CompareExchange;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.Extensions;
using Raven.Client.Http;
using Raven.Server.Documents.Includes;
using Raven.Server.Documents.Includes.Sharding;
using Raven.Server.Documents.Queries.Revisions;
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
        private readonly ShardedDatabaseRequestHandler _handler;
        private readonly ShardedDatabaseContext _databaseContext;
        private readonly Dictionary<int, ShardLocator.IdsByShard<string>> _idsByShards;
        private readonly string[] _includePaths;
        private readonly RevisionIncludeField _includeRevisions;
        private readonly StringValues _counterIncludes;
        private readonly HashSet<AbstractTimeSeriesRange> _timeSeriesIncludes;
        private readonly string[] _compareExchangeValueIncludes;
        private readonly bool _metadataOnly;

        public FetchDocumentsFromShardsOperation(TransactionOperationContext context,
            ShardedDatabaseRequestHandler handler,
            Dictionary<int, ShardLocator.IdsByShard<string>> idsByShards,
            string[] includePaths,
            RevisionIncludeField includeRevisions,
            StringValues counterIncludes,
            HashSet<AbstractTimeSeriesRange> timeSeriesIncludes,
            string[] compareExchangeValueIncludes,
            string etag,
            bool metadataOnly)
        {
            _context = context;
            _handler = handler;
            _databaseContext = handler.DatabaseContext;
            _idsByShards = idsByShards;
            ExpectedEtag = etag;
            _includePaths = includePaths;
            _includeRevisions = includeRevisions;
            _counterIncludes = counterIncludes;
            _timeSeriesIncludes = timeSeriesIncludes;
            _compareExchangeValueIncludes = compareExchangeValueIncludes;
            _metadataOnly = metadataOnly;
        }

        public string ExpectedEtag { get; }

        public GetShardedDocumentsResult CombineResults(Memory<GetDocumentsResult> results)
        {
            var span = results.Span;
            var docs = new Dictionary<string, BlittableJsonReaderObject>(StringComparer.OrdinalIgnoreCase);
            var includesMap = new Dictionary<string, BlittableJsonReaderObject>(StringComparer.OrdinalIgnoreCase);
            var missingIncludes = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            ShardedRevisionIncludes revisionIncludes = null;
            ShardedCounterIncludes counterIncludes = null;
            ShardedTimeSeriesIncludes timeSeriesIncludes = null;
            ShardedCompareExchangeValueInclude compareExchangeValueIncludes = null;

            foreach (var cmd in span)
            {
                if (cmd == null)
                    continue;

                var cmdResults = cmd.Results;
                var cmdIncludes = cmd.Includes;
                var cmdCounterIncludes = cmd.CounterIncludes;
                var cmdCompareExchangeValueIncludes = cmd.CompareExchangeValueIncludes;
                var cmdRevisionIncludes = cmd.RevisionIncludes;
                var cmdTimeSeriesIncludes = cmd.TimeSeriesIncludes;

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

                    if (cmdResult == null)
                        continue;

                    var docId = cmdResult.GetMetadata().GetId();
                    docs.TryAdd(docId, cmdResult.Clone(_context));
                }

                if (cmdRevisionIncludes != null)
                {
                    revisionIncludes ??= new ShardedRevisionIncludes();
                    revisionIncludes.AddResults(cmdRevisionIncludes, _context);
                }

                if (cmdCounterIncludes != null)
                {
                    counterIncludes ??= new ShardedCounterIncludes();
                    counterIncludes.AddResults(cmdCounterIncludes, null, _context);
                }

                if (cmdTimeSeriesIncludes != null)
                {
                    timeSeriesIncludes ??= new ShardedTimeSeriesIncludes(false);
                    timeSeriesIncludes.AddResults(cmdTimeSeriesIncludes, _context);
                }

                if (cmdCompareExchangeValueIncludes != null)
                {
                    compareExchangeValueIncludes ??= new ShardedCompareExchangeValueInclude();
                    compareExchangeValueIncludes.AddResults(cmdCompareExchangeValueIncludes, _context);
                }
            }

            foreach (var kvp in includesMap) // remove the items we already have
            {
                missingIncludes.Remove(kvp.Key);
            }

            return new GetShardedDocumentsResult
            {
                Documents = docs,
                Includes = includesMap.Values.ToList(),
                MissingDocumentIncludes = missingIncludes,
                RevisionIncludes = revisionIncludes,
                CounterIncludes = counterIncludes,
                TimeSeriesIncludes = timeSeriesIncludes,
                CompareExchangeValueIncludes = compareExchangeValueIncludes?.Results
            };
        }

        public HttpRequest HttpRequest => _handler.HttpContext.Request;

        public RavenCommand<GetDocumentsResult> CreateCommandForShard(int shardNumber) => new GetDocumentsCommand(_idsByShards[shardNumber].Ids.ToArray(), _includePaths,
            counterIncludes: _counterIncludes.Count > 0 ? _counterIncludes.ToArray() : null,
            revisionsIncludesByChangeVector: _includeRevisions?.RevisionsChangeVectorsPaths,
            revisionIncludeByDateTimeBefore: _includeRevisions?.RevisionsBeforeDateTime,
            timeSeriesIncludes: _timeSeriesIncludes,
            compareExchangeValueIncludes: _compareExchangeValueIncludes, 
            _metadataOnly);
    }

    public class GetShardedDocumentsResult
    {
        public List<BlittableJsonReaderObject> Includes;
        public Dictionary<string, BlittableJsonReaderObject> Documents;

        public HashSet<string> MissingDocumentIncludes;

        public IRevisionIncludes RevisionIncludes;

        public ICounterIncludes CounterIncludes;

        public ITimeSeriesIncludes TimeSeriesIncludes;

        public Dictionary<string, CompareExchangeValue<BlittableJsonReaderObject>> CompareExchangeValueIncludes;
    }
}
