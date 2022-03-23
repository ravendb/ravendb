using System;
using Microsoft.AspNetCore.Http;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Http;

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
        public RavenCommand<OperationState> CreateCommandForShard(int shard) => new GetOperationStateOperation.GetOperationStateCommand(_id, _nodeTag);

        public HttpRequest HttpRequest => _httpContext.Request;

        public OperationState Combine(Memory<OperationState> results)
        {
            var combined = new OperationState();

            OperationMultipleExceptionsResult operationExceptionsResult = null;
            SmugglerResult smugglerResult = null;

            var span = results.Span;

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
                    default:
                        throw new ArgumentException($"Not supported type {result.GetType()}");
                }
            }

            return combined;
        }
    }
}
