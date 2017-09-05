namespace Raven.Client.Documents.Session
{
    public interface ISessionInfo
    {
        int? SessionId { get;}

        bool AsyncCommandRunning { get; set; }
    }
}
