namespace Raven.Quill.Slack;

internal sealed class SlackChannelHealth
{
    public bool SocketConnected { get; private set; }
    public DateTime? LastConnectedAt { get; private set; }
    public string? LastSocketError { get; private set; }
    public DateTime? LastInboundAt { get; private set; }
    public DateTime? LastSendErrorAt { get; private set; }
    public string? LastSendError { get; private set; }

    public void Connected()
    {
        SocketConnected = true;
        LastConnectedAt = DateTime.UtcNow;
        LastSocketError = null;
    }

    public void Exited(string? error)
    {
        SocketConnected = false;
        LastSocketError = error;
    }

    public void Inbound() => LastInboundAt = DateTime.UtcNow;

    public void SendFailed(string error)
    {
        LastSendErrorAt = DateTime.UtcNow;
        LastSendError = error;
    }
}
