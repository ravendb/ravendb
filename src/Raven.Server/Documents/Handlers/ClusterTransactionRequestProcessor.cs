using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Operations.CompareExchange;
using Raven.Client.Exceptions;
using Raven.Client.Json;
using Raven.Client.ServerWide;
using Raven.Server.Rachis;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands;
using Raven.Server.Web;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers
{
    public abstract class BaseClusterTransactionRequestProcessor
    {
        private readonly RequestHandler _handler;
        protected readonly string Database;
        protected readonly char IdentitySeparator;

        protected BaseClusterTransactionRequestProcessor(RequestHandler handler, string database, char identitySeparator)
        {
            _handler = handler;
            Database = database;
            IdentitySeparator = identitySeparator;
        }

        public async Task Process(JsonOperationContext context, ArraySegment<BatchRequestParser.CommandData> parsedCommands)
        {
            var waitForIndexesTimeout = _handler.GetTimeSpanQueryString("waitForIndexesTimeout", required: false);
            var waitForIndexThrow = _handler.GetBoolValueQueryString("waitForIndexThrow", required: false) ?? true;
            var specifiedIndexesQueryString = _handler.HttpContext.Request.Query["waitForSpecificIndex"];

            ClusterTransactionCommand.ValidateCommands(parsedCommands);

            using (_handler.ServerStore.Cluster.ClusterTransactionWaiter.CreateTask(out var taskId))
            {
                var disableAtomicDocumentWrites = _handler.GetBoolValueQueryString("disableAtomicDocumentWrites", required: false) ?? false;

                CheckBackwardCompatibility(ref disableAtomicDocumentWrites);

                var options = new ClusterTransactionCommand.ClusterTransactionOptions(taskId, disableAtomicDocumentWrites, ClusterCommandsVersionManager.CurrentClusterMinimalVersion)
                {
                    WaitForIndexesTimeout = waitForIndexesTimeout,
                    WaitForIndexThrow = waitForIndexThrow,
                    SpecifiedIndexesQueryString = specifiedIndexesQueryString.Count > 0 ? specifiedIndexesQueryString.ToList() : null
                };

                var raftRequestId = _handler.GetRaftRequestIdFromQuery();
                ClusterTransactionCommand clusterTransactionCommand = CreateClusterTransactionCommand(parsedCommands, options, raftRequestId);

                (long index, object clusterTransactionCommandResult) = await _handler.ServerStore.SendToLeaderAsync(clusterTransactionCommand);
                if (clusterTransactionCommandResult is List<ClusterTransactionCommand.ClusterTransactionErrorInfo> errors)
                {
                    _handler.HttpContext.Response.StatusCode = (int)HttpStatusCode.Conflict;
                    throw new ClusterTransactionConcurrencyException($"Failed to execute cluster transaction due to the following issues: {string.Join(Environment.NewLine, errors.Select(e => e.Message))}")
                    {
                        ConcurrencyViolations = errors.Select(e => e.Violation).ToArray()
                    };
                }
                await _handler.ServerStore.WaitForCommitIndexChange(RachisConsensus.CommitIndexModification.GreaterOrEqual, index);

                DynamicJsonArray result;
                if (clusterTransactionCommand.DatabaseCommands.Count > 0)
                {
                    using var timeout = new CancellationTokenSource(_handler.ServerStore.Engine.OperationTimeout);
                    using var cts = CancellationTokenSource.CreateLinkedTokenSource(timeout.Token, _handler.HttpContext.RequestAborted);
                    var databaseResult = await _handler.ServerStore.Cluster.ClusterTransactionWaiter.WaitForResults(taskId, cts.Token);
                    result = databaseResult.Array;
                }
                else
                {
                    result = new DynamicJsonArray();
                }

                if (clusterTransactionCommand.ClusterCommands.Count > 0)
                {
                    foreach (var clusterCommands in clusterTransactionCommand.ClusterCommands)
                    {
                        result.Add(new DynamicJsonValue
                        {
                            [nameof(ICommandData.Type)] = clusterCommands.Type,
                            [nameof(ICompareExchangeValue.Key)] = clusterCommands.Id,
                            [nameof(ICompareExchangeValue.Index)] = index
                        });
                    }
                }

                _handler.HttpContext.Response.StatusCode = (int)HttpStatusCode.Created;
                await using (var writer = new AsyncBlittableJsonTextWriter(context, _handler.ResponseBodyStream()))
                {
                    context.Write(writer,
                        new DynamicJsonValue
                        {
                            [nameof(BatchCommandResult.Results)] = result,
                            [nameof(BatchCommandResult.TransactionIndex)] = index
                        });
                }
            }
        }

        protected abstract ClusterTransactionCommand CreateClusterTransactionCommand(ArraySegment<BatchRequestParser.CommandData> parsedCommands,
            ClusterTransactionCommand.ClusterTransactionOptions options, string raftRequestId);

        private void CheckBackwardCompatibility(ref bool disableAtomicDocumentWrites)
        {
            if (disableAtomicDocumentWrites)
                return;

            if (RequestRouter.TryGetClientVersion(_handler.HttpContext, out var clientVersion) == false)
            {
                disableAtomicDocumentWrites = true;
                return;
            }

            if (clientVersion.Major < 5 || (clientVersion.Major == 5 && clientVersion.Minor < 2))
            {
                disableAtomicDocumentWrites = true;
            }
        }
    }

    class ClusterTransactionRequestProcessor : BaseClusterTransactionRequestProcessor
    {
        private readonly DatabaseTopology _topology;

        public ClusterTransactionRequestProcessor(RequestHandler handler, string database, char identitySeparator, DatabaseTopology topology) : base(handler, database, identitySeparator)
        {
            _topology = topology;
        }

        protected override ClusterTransactionCommand CreateClusterTransactionCommand(
            ArraySegment<BatchRequestParser.CommandData> parsedCommands, 
            ClusterTransactionCommand.ClusterTransactionOptions options, 
            string raftRequestId)
        {
            return new ClusterTransactionCommand(
                Database,
                IdentitySeparator,
                _topology,
                parsedCommands,
                options,
                raftRequestId);
        }
    }
    
    class ShardClusterTransactionRequestProcessor : BaseClusterTransactionRequestProcessor
    {
        public ShardClusterTransactionRequestProcessor(RequestHandler handler, string database, char identitySeparator) : base(handler, database, identitySeparator)
        {
        }

        protected override ClusterTransactionCommand CreateClusterTransactionCommand(
            ArraySegment<BatchRequestParser.CommandData> parsedCommands, 
            ClusterTransactionCommand.ClusterTransactionOptions options, 
            string raftRequestId)
        {
            return new ClusterTransactionCommand(
                Database,
                IdentitySeparator,
                parsedCommands,
                options,
                raftRequestId);
        }
    }
}
