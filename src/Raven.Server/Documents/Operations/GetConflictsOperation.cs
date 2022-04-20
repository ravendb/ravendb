using System;
using System.Net.Http;
using System.Text;
using JetBrains.Annotations;
using NCrontab.Advanced.Extensions;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Http;
using Raven.Server.Documents.Sharding.Handlers.ContinuationTokens;
using Raven.Server.Json;
using Sparrow.Json;

namespace Raven.Server.Documents.Operations
{
    internal class GetConflictsOperation : IMaintenanceOperation<GetConflictsPreviewResult>
    {
        private readonly long _start;
        private readonly int? _pageSize;
        [CanBeNull]
        private readonly string _token;

        public GetConflictsOperation(long start = 0)
        {
            _start = start;
        }

        public GetConflictsOperation(long start, int pageSize)
        {
            _start = start;
            _pageSize = pageSize;
        }

        public GetConflictsOperation(string continuationToken)
        {
            _token = continuationToken;
        }

        public RavenCommand<GetConflictsPreviewResult> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            if (_token.IsNullOrWhiteSpace() == false)
                return new GetConflictsCommand(_token);

            if (_pageSize.HasValue)
                return new GetConflictsCommand(_start, _pageSize.Value);

            return new GetConflictsCommand(_start);
        }

        internal class GetConflictsCommand : RavenCommand<GetConflictsPreviewResult>
        {
            private readonly long _start;
            private readonly int? _pageSize;
            [CanBeNull]
            private readonly string _token;

            public override bool IsReadRequest => true;

            public GetConflictsCommand(long start = 0)
            {
                _start = start;
            }

            public GetConflictsCommand(long start = 0, int pageSize = int.MaxValue)
            {
                _start = start;
                _pageSize = pageSize;
            }

            public GetConflictsCommand(string continuationToken)
            {
                _token = continuationToken;
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                var sb = new StringBuilder();
                sb.Append($"{node.Url}/databases/{node.Database}/replication/conflicts");

                if (_token != null)
                    sb.Append($"?{ContinuationToken.ContinuationTokenQueryString}={Uri.EscapeDataString(_token)}");
                else
                {
                    sb.Append($"?start={_start}");

                    if (_pageSize.HasValue && _pageSize != default)
                        sb.Append($"&pageSize={_pageSize}");
                }

                url = sb.ToString();

                var request = new HttpRequestMessage { Method = HttpMethod.Get };

                return request;
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    ThrowInvalidResponse();

                Result = JsonDeserializationServer.GetConflictResults(response);
            }
        }
    }
}
