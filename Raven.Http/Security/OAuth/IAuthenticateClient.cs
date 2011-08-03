namespace Raven.Http.Security.OAuth
{
    public interface IAuthenticateClient
    {
        bool Authenticate(IResourceStore currentStore, string username, string password);
    }
}