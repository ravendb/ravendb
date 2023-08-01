using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using Microsoft.AspNetCore.Http;
using Raven.Client;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Extensions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Serialization;
using Raven.Server.Documents.Sharding.Executors;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Utils;

namespace Raven.Server.Documents.Sharding.Operations;

public readonly struct ShardedSubscriptionTryoutOperation : IShardedOperation<GetDocumentsResult>
{
    private readonly HttpContext _httpContext;
    private readonly TransactionOperationContext _context;
    private readonly SubscriptionTryout _tryout;
    private readonly int _pageSize;
    private readonly int? _timeLimitInSec;

    public ShardedSubscriptionTryoutOperation(HttpContext httpContext, TransactionOperationContext context, SubscriptionTryout tryout, int pageSize, int? timeLimitInSec)
    {
        _httpContext = httpContext;
        _context = context;
        _tryout = tryout;
        _pageSize = pageSize;
        _timeLimitInSec = timeLimitInSec;
    }

    public HttpRequest HttpRequest => _httpContext.Request;

    public GetDocumentsResult Combine(Dictionary<int, ShardExecutionResult<GetDocumentsResult>> results)
    {
        var getDocumentsResult = new GetDocumentsResult();
        var objList = new List<BlittableJsonReaderObject>();

        foreach (var (shardNumber, shardResult) in results)
        {
            foreach (BlittableJsonReaderObject obj in shardResult.Result.Results)
            {
                objList.Add(obj.AddToMetadata(_context, Constants.Documents.Metadata.Sharding.ShardNumber, shardNumber));
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

    public RavenCommand<GetDocumentsResult> CreateCommandForShard(int shardNumber)
    {
        return new SubscriptionTryoutCommand(_tryout, _pageSize, _timeLimitInSec);
    }

    private sealed class SubscriptionTryoutCommand : RavenCommand<GetDocumentsResult>
    {
        private readonly SubscriptionTryout _tryout;
        private readonly int _pageSize;
        private readonly int? _timeLimitInSec;

        public SubscriptionTryoutCommand(SubscriptionTryout tryout, int pageSize, int? timeLimitInSec)
        {
            _tryout = tryout;
            _pageSize = pageSize;
            _timeLimitInSec = timeLimitInSec;
        }

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/databases/{node.Database}/subscriptions/try?pageSize={_pageSize}";
            if (_timeLimitInSec.HasValue)
            {
                url += $"&timeLimit={_timeLimitInSec}";
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
                }, DocumentConventions.DefaultForServer)
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
