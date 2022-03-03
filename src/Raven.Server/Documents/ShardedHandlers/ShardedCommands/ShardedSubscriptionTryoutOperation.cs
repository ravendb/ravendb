using System;
using System.Collections.Generic;
using System.Net.Http;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Serialization;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Utils;

namespace Raven.Server.Documents.ShardedHandlers.ShardedCommands
{
    public readonly struct ShardedSubscriptionTryoutOperation : IShardedOperation<GetDocumentsResult>
    {
        private readonly TransactionOperationContext _context;
        private readonly SubscriptionTryout _tryout;
        private readonly int _pageSize;
        private readonly int? _timeLimit;

        public ShardedSubscriptionTryoutOperation(TransactionOperationContext context, SubscriptionTryout tryout, int pageSize, int? timeLimit)
        {
            _context = context;
            _tryout = tryout;
            _pageSize = pageSize;
            _timeLimit = timeLimit;
        }

        public GetDocumentsResult Combine(Memory<GetDocumentsResult> results)
        {
            var getDocumentsResult = new GetDocumentsResult();
            var objList = new List<BlittableJsonReaderObject>();

            foreach (var res in results.ToArray())
            {
                foreach (BlittableJsonReaderObject obj in res.Results)
                {
                    objList.Add(obj.Clone(_context));
                }
            }

            var bjro = _context.ReadObject(new DynamicJsonValue()
            {
                ["Results"] = objList
            }, "Combine-GetDocumentsResult");

            getDocumentsResult.Results = (BlittableJsonReaderArray)bjro["Results"];
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Egor, DevelopmentHelper.Severity.Major, "https://issues.hibernatingrhinos.com/issue/RavenDB-16279");

            return getDocumentsResult;
        }

        public RavenCommand<GetDocumentsResult> CreateCommandForShard(int shard)
        {
            return new SubscriptionTryoutCommand(_tryout, _pageSize, _timeLimit);
        }

        private class SubscriptionTryoutCommand : RavenCommand<GetDocumentsResult>
        {
            private readonly SubscriptionTryout _tryout;
            private readonly int _pageSize;
            private readonly int? _timeLimit;

            public SubscriptionTryoutCommand(SubscriptionTryout tryout, int pageSize, int? timeLimit)
            {
                _tryout = tryout;
                _pageSize = pageSize;
                _timeLimit = timeLimit;
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/subscriptions/try?pageSize={_pageSize}";
                if (_timeLimit.HasValue)
                {
                    url += $"&timeLimit={_timeLimit}";
                }

                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Post,
                    Content = new BlittableJsonContent(async stream =>
                    {
                        await using (var writer = new AsyncBlittableJsonTextWriter(ctx, stream))
                        {
                            writer.WriteStartObject();
                            writer.WritePropertyName(nameof(SubscriptionTryout.ChangeVector));
                            writer.WriteString(_tryout.ChangeVector);
                            writer.WritePropertyName(nameof(SubscriptionTryout.Query));
                            writer.WriteString(_tryout.Query);
                            writer.WriteEndObject();
                        }
                    })
                };

                return request;
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                {
                    Result = null;
                    return;
                }

                Result = JsonDeserializationClient.GetDocumentsResult(response);
            }

            public override bool IsReadRequest => true;
        }
    }
}
