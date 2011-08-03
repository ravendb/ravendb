using System.ComponentModel.Composition;

namespace Raven.Http.Security.OAuth
{
    [Export(typeof(IAuthenticateClient))]
    public class AuthenticateClient : IAuthenticateClient
    {
        public bool Authenticate(string username, string password)
        {
            //TODO: Authenticate using OAuthUser documents in default database
            return !string.IsNullOrEmpty(password);
        }
    }
}