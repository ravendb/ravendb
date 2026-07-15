namespace Raven.Quill.Hosting;

public interface IServerReady
{
    bool IsReady { get; }
    string? LastError { get; }
    void MarkReady();
    void MarkFailed(string error);
}

public sealed class ServerReadyFlag : IServerReady
{
    private int _ready;
    private string? _lastError;

    public bool IsReady => Volatile.Read(ref _ready) == 1;
    public string? LastError => Volatile.Read(ref _lastError);

    public void MarkReady()
    {
        Volatile.Write(ref _lastError, null);
        Volatile.Write(ref _ready, 1);
    }

    public void MarkFailed(string error)
    {
        Volatile.Write(ref _lastError, error);
        Volatile.Write(ref _ready, 0);
    }
}
