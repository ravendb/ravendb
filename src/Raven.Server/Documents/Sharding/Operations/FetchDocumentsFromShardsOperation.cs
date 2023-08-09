using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Primitives;
using Raven.Client;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Operations.CompareExchange;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.Documents.Session;
using Raven.Client.Extensions;
using Raven.Client.Http;
using Raven.Server.Documents.Includes;
using Raven.Server.Documents.Includes.Sharding;
using Raven.Server.Documents.Queries.Revisions;
using Raven.Server.Documents.Sharding.Executors;
using Raven.Server.Extensions;
using Raven.Server.Utils;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Operations
{
    public readonly struct FetchDocumentsFromShardsOperation : IShardedReadOperation<GetDocumentsResult, GetShardedDocumentsResult>
    {
        private readonly JsonOperationContext _context;
        private readonly ShardedDatabaseContext _databaseContext;
        private readonly Dictionary<int, ShardLocator.IdsByShard<string>> _idsByShards;
        private readonly string[] _includePaths;
        private readonly RevisionIncludeField _includeRevisions;
        private readonly StringValues _counterIncludes;
        private readonly HashSet<AbstractTimeSeriesRange> _timeSeriesIncludes;
        private readonly string[] _compareExchangeValueIncludes;
        private readonly bool _metadataOnly;
        private readonly bool _clusterWideTx;

        public FetchDocumentsFromShardsOperation(JsonOperationContext context,
            HttpRequest httpRequest,
            ShardedDatabaseContext databaseContext,
            Dictionary<int, ShardLocator.IdsByShard<string>> idsByShards,
            string[] includePaths,
            RevisionIncludeField includeRevisions,
            StringValues counterIncludes,
            HashSet<AbstractTimeSeriesRange> timeSeriesIncludes,
            string[] compareExchangeValueIncludes,
            string etag,
            bool metadataOnly,
            bool clusterWideTx)
        {
            _context = context;
            HttpRequest = httpRequest;
            _databaseContext = databaseContext;
            _idsByShards = idsByShards;
            ExpectedEtag = etag;
            _includePaths = includePaths;
            _includeRevisions = includeRevisions;
            _counterIncludes = counterIncludes;
            _timeSeriesIncludes = timeSeriesIncludes;
            _compareExchangeValueIncludes = compareExchangeValueIncludes;
            _metadataOnly = metadataOnly;
            _clusterWideTx = clusterWideTx;
        }

        public HttpRequest HttpRequest { get; }
        public string ExpectedEtag { get; }

        public GetShardedDocumentsResult CombineResults(Dictionary<int, ShardExecutionResult<GetDocumentsResult>> results)
        {
            var docs = new Dictionary<string, BlittableJsonReaderObject>(StringComparer.OrdinalIgnoreCase);
            var includesMap = new Dictionary<string, BlittableJsonReaderObject>(StringComparer.OrdinalIgnoreCase);
            var missingIncludes = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            var fromStudio = HttpRequest?.IsFromStudio() ?? false;

            ShardedRevisionIncludes revisionIncludes = null;
            ShardedCounterIncludes counterIncludes = null;
            ShardedTimeSeriesIncludes timeSeriesIncludes = null;
            ShardedCompareExchangeValueInclude compareExchangeValueIncludes = null;

            foreach (var (shardNumber, cmd) in results)
            {
                var docRes = cmd.Result;
                if (docRes == null)
                    continue;

                var cmdResults = docRes.Results;
                var cmdIncludes = docRes.Includes;
                var cmdCounterIncludes = docRes.CounterIncludes;
                var cmdCompareExchangeValueIncludes = docRes.CompareExchangeValueIncludes;
                var cmdRevisionIncludes = docRes.RevisionIncludes;
                var cmdTimeSeriesIncludes = docRes.TimeSeriesIncludes;

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
                    if (cmdResult == null)
                        continue;

                    if (_includePaths != null)
                    {
                        foreach (string includePath in _includePaths)
                            IncludeUtil.GetDocIdFromInclude(cmdResult, includePath, missingIncludes, _databaseContext.IdentityPartsSeparator);
                    }

                    var docId = cmdResult.GetMetadata().GetId();
                    var result = fromStudio ?
                        cmdResult.AddToMetadata(_context, Constants.Documents.Metadata.Sharding.ShardNumber, shardNumber) :
                        cmdResult.Clone(_context);
                    docs.TryAdd(docId, result);
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

        public RavenCommand<GetDocumentsResult> CreateCommandForShard(int shardNumber)
        {
            var cmd = new GetDocumentsCommand(_databaseContext.ShardExecutor.Conventions, _idsByShards[shardNumber].Ids.ToArray(), _includePaths,
                counterIncludes: _counterIncludes.Count > 0 ? _counterIncludes.ToArray() : null,
                revisionsIncludesByChangeVector: _includeRevisions?.RevisionsChangeVectorsPaths,
                revisionIncludeByDateTimeBefore: _includeRevisions?.RevisionsBeforeDateTime,
                timeSeriesIncludes: _timeSeriesIncludes,
                compareExchangeValueIncludes: _compareExchangeValueIncludes,
                _metadataOnly);

            if (_clusterWideTx)
                cmd.SetTransactionMode(TransactionMode.ClusterWide);

            return cmd;
        }
    }

    public sealed class GetShardedDocumentsResult
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
