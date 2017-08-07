using System;
using System.Collections.Generic;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations
{
    public class GetIdentitiesCommand : RavenCommand<Dictionary<string, long>>
    {
        private static readonly Func<BlittableJsonReaderObject, IdentitiesResult> _deserializeIdentities = 
            JsonDeserializationBase.GenerateJsonDeserializationRoutine<IdentitiesResult>();

        // ReSharper disable once ClassNeverInstantiated.Local
        private class IdentitiesResult
        {
            public Dictionary<string, long> Identities { get; set; }
        }

        public override bool IsReadRequest => true;

        public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
        {
            url = $"{node.Url}/databases/{node.Database}/debug/identities";
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

    public class GetIdentitiesOperation : IAdminOperation<Dictionary<string,long>>
    {
        public RavenCommand<Dictionary<string, long>> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new GetIdentitiesCommand();
        }
    }
}
