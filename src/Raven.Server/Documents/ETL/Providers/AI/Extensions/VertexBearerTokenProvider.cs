using System;
using System.Threading.Tasks;
using Google.Apis.Auth.OAuth2;
using Raven.Client.Documents.Operations.AI;

namespace Raven.Server.Documents.ETL.Providers.AI.Extensions;

public class VertexBearerTokenProvider
{
    public readonly Func<ValueTask<string>> BearerTokenProvider;
    public const string GoogleCloudPlatformUrl = "https://www.googleapis.com/auth/cloud-platform";

    public VertexBearerTokenProvider(VertexSettings vertexSettings)
    {
        BearerTokenProvider = async () =>
        {
            var credential = GoogleCredential.FromJson(vertexSettings.GoogleCredentialsJson)
                .CreateScoped(GoogleCloudPlatformUrl);
            
            ITokenAccess tokenAccess = credential;
            string token = await tokenAccess.GetAccessTokenForRequestAsync();
                
            return token ?? throw new InvalidOperationException("Failed to retrieve a valid bearer token.");
        };
    }
}
