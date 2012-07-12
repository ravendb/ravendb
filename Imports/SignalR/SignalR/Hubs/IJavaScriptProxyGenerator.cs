namespace Raven.Imports.SignalR.Hubs
{
    public interface IJavaScriptProxyGenerator
    {
        string GenerateProxy(string serviceUrl);
    }
}
