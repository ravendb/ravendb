namespace Raven.Quill.Channels;

internal sealed class ChannelConnectionHealth
{
    public bool IsConnected { get; private set; }
    public DateTime? LastConnectedAt { get; private set; }
    public string? LastConnectionError { get; private set; }
    public DateTime? LastInboundAt { get; private set; }
    public DateTime? LastSendErrorAt { get; private set; }
    public string? LastSendError { get; private set; }

    public void Connected()
    {
        IsConnected = true;
        LastConnectedAt = DateTime.UtcNow;
        LastConnectionError = null;
    }

    public void Disconnected(string? error)
    {
        IsConnected = false;
        if (error is not null)
            LastConnectionError = error;
    }

    public void Inbound() => LastInboundAt = DateTime.UtcNow;

    public void SendFailed(string error)
    {
        LastSendErrorAt = DateTime.UtcNow;
        LastSendError = error;
    }

    public void SendSucceeded()
    {
        LastSendErrorAt = null;
        LastSendError = null;
    }
}
