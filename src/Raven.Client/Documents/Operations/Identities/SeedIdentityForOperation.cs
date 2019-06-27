using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.Identities
{
    public class SeedIdentityForOperation : IMaintenanceOperation<long>
    {
        private readonly string _identityName;
        private readonly long _identityValue;
        private readonly bool _forceUpdate;

        public SeedIdentityForOperation(string identityName, long identityValue)
        {
            _identityName = identityName;
            _identityValue = identityValue;
            _forceUpdate = false;
        }

        public SeedIdentityForOperation(string identityName, long identityValue, bool forceUpdate)
        {
            _identityName = identityName;
            _identityValue = identityValue;
            _forceUpdate = forceUpdate;
        }

        public RavenCommand<long> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new SeedIdentityForCommand(_identityName, _identityValue, _forceUpdate);
        }

        private class SeedIdentityForCommand : RavenCommand<long>, IRaftCommand
        {
            private readonly string _id;
            private readonly long _value;
            private readonly bool _forced;

            public SeedIdentityForCommand(string id, long value, bool forced)
            {
                _id = id ?? throw new ArgumentNullException(nameof(id));
                _value = value;
                _forced = forced;
            }

            public override bool IsReadRequest { get; } = false;

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                EnsureIsNotNullOrEmpty(_id, nameof(_id));

                url = $"{node.Url}/databases/{node.Database}/identity/seed?name={UrlEncode(_id)}&value={_value}";
                if (_forced)
                {
                    url += $"&force={_forced}";
                }
                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Post
                };
                return request;
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null || response.TryGet("NewSeedValue", out long result) == false)
                {
                    ThrowInvalidResponse();
                    return; // never hit
                }


                Result = result;
            }

            public string RaftUniqueRequestId { get; } = RaftIdGenerator.NewId();
        }
    }
}
