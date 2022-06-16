using System;
using System.Net.Http;
using System.Text;
using Raven.Client.Http;
using Raven.Server.Json;
using Sparrow.Extensions;
using Sparrow.Json;

namespace Raven.Server.Documents.Commands.Revisions
{
    public class GetResolvedRevisionsCommand : RavenCommand<ResolvedRevisions>
    {
        private readonly DateTime? _since;
        private readonly int? _take;

        public GetResolvedRevisionsCommand(DateTime? since, int? take = null)
        {
            _since = since;
            _take = take;
        }
        
        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Get
            };

            var pathBuilder = new StringBuilder(node.Url)
                .Append("/databases/")
                .Append(node.Database)
                .Append("/revisions/resolved");

            if (_since.HasValue && _take.HasValue)
            {
                pathBuilder.Append("?since=").Append(_since.Value.GetDefaultRavenFormat());
                pathBuilder.Append("&take=").Append(_take.Value);
            }
            else if (_take.HasValue) 
                pathBuilder.Append("?take=").Append(_take.Value);
            else if (_since.HasValue)
                pathBuilder.Append("?since=").Append(_since.Value.GetDefaultRavenFormat());

            url = pathBuilder.ToString();
            return request;
        }

        public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
        {
            if (response == null)
            {
                Result = null;
                return;
            }
            
            Result = JsonDeserializationServer.ResolvedRevisions(response);
        }

        public override bool IsReadRequest => true;
    }

    public class ResolvedRevisions
    {
        public BlittableJsonReaderArray Results { get; set; }
    }
}
