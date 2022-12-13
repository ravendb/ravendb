using System;
using System.Collections.Generic;
using Microsoft.AspNetCore.Http;
using Raven.Client.Documents.Operations;
using Raven.Client.Http;
using Raven.Server.Documents.Sharding.Executors;
using Sparrow.Utils;

namespace Raven.Server.Documents.Sharding.Operations
{
    public readonly struct GetShardedOperationStateOperation : IShardedOperation<OperationState>
    {
        private readonly HttpContext _httpContext;
        private readonly long _id;
        private readonly string _nodeTag;

        public GetShardedOperationStateOperation(HttpContext httpContext, long id, string nodeTag = null)
        {
            _httpContext = httpContext;
            _id = id;
            _nodeTag = nodeTag;
        }
        public RavenCommand<OperationState> CreateCommandForShard(int shardNumber) => new GetOperationStateOperation.GetOperationStateCommand(_id, _nodeTag);

        public HttpRequest HttpRequest => _httpContext.Request;

        public OperationState Combine(Dictionary<int, ShardExecutionResult<OperationState>> results)
        {
            var combined = new OperationState();

            OperationMultipleExceptionsResult operationExceptionsResult = null;
            BulkOperationResult bulkResult = null;
            
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Arek, DevelopmentHelper.Severity.Normal, "RavenDB-19082 We might get different operations states from different nodes e.g. two BulkOperationResults which succeeded and one OperationExceptionResult");

            foreach (var shardResult in results.Values)
            {
                var operationResult = shardResult.Result.Result;

                switch (operationResult)
                {
                    case OperationExceptionResult operationException:
                        if (operationExceptionsResult == null)
                        {
                            operationExceptionsResult = new OperationMultipleExceptionsResult("RavenDB-19082 Operation has failed with multiple errors");
                            combined.Result = operationExceptionsResult;
                        }

                        operationExceptionsResult.Exceptions.Add(operationException);

                        break;
                    case BulkOperationResult bulk:
                        if (bulkResult == null)
                        {
                            bulkResult = new BulkOperationResult();
                            combined.Result = bulkResult;
                        }

                        bulkResult.AttachmentsProcessed += bulk.AttachmentsProcessed;
                        bulkResult.CountersProcessed += bulk.CountersProcessed;
                        bulkResult.DocumentsProcessed += bulk.DocumentsProcessed;
                        bulkResult.Details.AddRange(bulk.Details);
                        bulkResult.Query = bulk.Query;
                        bulkResult.TimeSeriesProcessed += bulk.TimeSeriesProcessed;
                        bulkResult.Total += bulk.Total;

                        break;
                    default:
                        throw new ArgumentException($"Not supported operation type result {operationResult.GetType()}");
                }
            }

            return combined;
        }
    }
}
