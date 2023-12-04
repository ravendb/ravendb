using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json;
using Sparrow.Json;

namespace Raven.Client.ServerWide.Operations.Certificates;

public class ValidateTwoFactorAuthenticationTokenOperation : IServerOperation<string>
{
    private readonly string _validationCode;

    internal bool WithLimits;

    public ValidateTwoFactorAuthenticationTokenOperation(string validationCode)
    {
        _validationCode = validationCode;
    }
    
    public RavenCommand<string> GetCommand(DocumentConventions conventions, JsonOperationContext context)
    {
        return new ValidateTwoFactorAuthenticationTokenCommand(_validationCode, WithLimits);
    }
    
    private class ValidateTwoFactorAuthenticationTokenCommand : RavenCommand<string>
    {
        private readonly string _validationCode;
        private readonly bool _withLimits;

        public override bool IsReadRequest => false;

        public ValidateTwoFactorAuthenticationTokenCommand(string validationCode, bool withLimits)
        {
            _validationCode = validationCode;
            _withLimits = withLimits;
        }

        public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
        {
            response.TryGet("Token", out Result);
        }

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/authentication/2fa?hasLimits=" + _withLimits;

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
