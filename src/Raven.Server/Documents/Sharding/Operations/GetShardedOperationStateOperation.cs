using System;
using Microsoft.AspNetCore.Http;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Http;
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

        public OperationState Combine(Memory<OperationState> results)
        {
            var combined = new OperationState();

            OperationMultipleExceptionsResult operationExceptionsResult = null;
            SmugglerResult smugglerResult = null;
            BulkOperationResult bulkResult = null;

            var span = results.Span;

            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Arek, DevelopmentHelper.Severity.Normal, "We might get different operations states from different nodes e.g. two BulkOperationResults which succeeded and one OperationExceptionResult");

            for (int i = 0; i < results.Length; i++)
            {
                var result = span[i].Result;

                switch (result)
                {
                    case OperationExceptionResult operationException:
                        if (operationExceptionsResult == null)
                        {
                            operationExceptionsResult = new OperationMultipleExceptionsResult("Operation has failed with multiple errors");
                            combined.Result = operationExceptionsResult;
                        }

                        operationExceptionsResult.Exceptions.Add(operationException);

                        break;
                    case SmugglerResult smuggler:
                        if (smugglerResult == null)
                        {
                            smugglerResult = new SmugglerResult();
                            combined.Result = smugglerResult;
                        }

                        GetOperationStateOperation.GetOperationStateCommand.CombineSmugglerResults(combined.Result, smuggler);
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
                        throw new ArgumentException($"Not supported operation type result {result.GetType()}");
                }
            }

            return combined;
        }
    }
}
