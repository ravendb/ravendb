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
    internal class GetConflictsByEtagOperation : IMaintenanceOperation<GetConflictsResultByEtag>
    {
        private readonly long _etag;
        private readonly int? _pageSize;
        [CanBeNull]
        private readonly string _token;

        public GetConflictsByEtagOperation(long etag = 0)
        {
            _etag = etag;
        }

        public GetConflictsByEtagOperation(long etag, int pageSize)
        {
            _etag = etag;
            _pageSize = pageSize;
        }

        public GetConflictsByEtagOperation(string continuationToken)
        {
            _token = continuationToken;
        }

        public RavenCommand<GetConflictsResultByEtag> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            if (_token.IsNullOrWhiteSpace() == false)
                return new GetConflictsByEtagCommand(_token);

            if (_pageSize.HasValue)
                return new GetConflictsByEtagCommand(_etag, _pageSize.Value);

            return new GetConflictsByEtagCommand(_etag);
        }

        internal class GetConflictsByEtagCommand : RavenCommand<GetConflictsResultByEtag>
        {
            private readonly long _etag;
            private readonly int? _pageSize;
            [CanBeNull]
            private readonly string _token;

            public override bool IsReadRequest => true;

            public GetConflictsByEtagCommand(long etag = 0)
            {
                _etag = etag;
            }

            public GetConflictsByEtagCommand(long etag = 0, int pageSize = int.MaxValue)
            {
                _etag = etag;
                _pageSize = pageSize;
            }

            public GetConflictsByEtagCommand(string continuationToken)
            {
                _etag = 0;
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
                    sb.Append($"?etag={_etag}");

                    if (_pageSize.HasValue)
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
