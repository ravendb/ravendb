namespace Raven.Quill.Channels;

internal sealed class ChannelConnectionHealth
{
    public bool IsConnected { get; private set; }
    public DateTime? LastConnectedAt { get; private set; }
    public string? LastConnectionError { get; private set; }
    public DateTime? LastInboundAt { get; private set; }
    public DateTime? LastSendErrorAt { get; private set; }
    public string? LastSendError { get; private set; }

    public void MarkConnected()
    {
        IsConnected = true;
        LastConnectedAt = DateTime.UtcNow;
        LastConnectionError = null;
    }

    public void MarkDisconnected(string? error)
    {
        IsConnected = false;
        if (error is not null)
            LastConnectionError = error;
    }

    public void MarkReceived() => LastInboundAt = DateTime.UtcNow;

    public void MarkSendFailed(string error)
    {
        LastSendErrorAt = DateTime.UtcNow;
        LastSendError = error;
    }

    public void MarkSendSucceeded()
    {
        LastSendErrorAt = null;
        LastSendError = null;
    }
}
