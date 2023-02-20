using System;
using System.Net.Http;
using System.Security.Cryptography.X509Certificates;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json;
using Sparrow.Json;

namespace Raven.Client.ServerWide.Operations.Certificates;

public class ValidateTwoFactorAuthenticationTokenOperation : IServerOperation<string>
{
    private readonly string _validationCode;

    public ValidateTwoFactorAuthenticationTokenOperation(string validationCode)
    {
        _validationCode = validationCode;
    }
    
    public RavenCommand<string> GetCommand(DocumentConventions conventions, JsonOperationContext context)
    {
        return new ValidateTwoFactorAuthenticationTokenCommand(_validationCode);
    }
    
    private class ValidateTwoFactorAuthenticationTokenCommand : RavenCommand<string>
    {
        private readonly string _validationCode;

        public override bool IsReadRequest => false;

        public ValidateTwoFactorAuthenticationTokenCommand(string validationCode)
        {
            _validationCode = validationCode;
        }

        public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
        {
            response.TryGet("Token", out Result);
        }

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/authentication/2fa";

            return new HttpRequestMessage
            {
                Method = HttpMethod.Post,
                Content = new BlittableJsonContent(async stream =>
                {
                    await using (var writer = new AsyncBlittableJsonTextWriter(ctx, stream))
                    {
                        writer.WriteStartObject();
                        writer.WritePropertyName("Token");
                        writer.WriteString(_validationCode);
                        writer.WriteEndObject();
                    }
                })
            };
        }
    }
}
