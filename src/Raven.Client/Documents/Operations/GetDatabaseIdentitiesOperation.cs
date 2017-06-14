using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Text;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations
{
    public class GetDatabaseIdentities : RavenCommand<Dictionary<string, long>>
    {
        private static readonly Func<BlittableJsonReaderObject, IdentitiesResult> _deserializeIdentities = 
            JsonDeserializationBase.GenerateJsonDeserializationRoutine<IdentitiesResult>();

        // ReSharper disable once ClassNeverInstantiated.Local
        private class IdentitiesResult
        {
            public Dictionary<string, long> Identities { get; set; }
        }

        public override bool IsReadRequest => true;

        public int Start { get; set; } = -1;

        public int PageSize { get; set; } = -1;

        public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
        {
            url = $"{node.Url}/databases/{node.Database}/debug/identities";

            if (Start != -1)
                url += $"&start={Start}";
            if (PageSize != -1)
                url += $"&pageSize={PageSize}";

            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Get
            };

            return request;
        }

        public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
        {
            Result = _deserializeIdentities(response).Identities;
        }
    }

    public class GetDatabaseIdentitiesOperation : IAdminOperation<Dictionary<string,long>>
    {
        public int Start { get; set; } = -1;

        public int PageSize { get; set; } = -1;

        public RavenCommand<Dictionary<string, long>> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new GetDatabaseIdentities
            {
                Start = this.Start,
                PageSize = this.PageSize
            };
        }
    }
}
