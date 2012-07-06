namespace Raven.Imports.SignalR
{
    public interface IRequestCookieCollection
    {
        Cookie this[string name] { get; }
        int Count { get; }
    }
}