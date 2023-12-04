using System;
using System.Net.Http;
using System.Security.Cryptography.X509Certificates;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json;
using Sparrow.Json;

namespace Raven.Client.ServerWide.Operations.Certificates;

public class ForgotTwoFactorAuthenticationTokenOperation : IServerOperation
{

    public RavenCommand GetCommand(DocumentConventions conventions, JsonOperationContext context)
    {
        return new ForgotTwoFactorAuthenticationTokenCommand();
    }
    
    private class ForgotTwoFactorAuthenticationTokenCommand : RavenCommand
    {
        public override bool IsReadRequest => false;
        

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/authentication/2fa";

            return new HttpRequestMessage
            {
                Method = HttpMethod.Delete
            };
        }
    }
}
