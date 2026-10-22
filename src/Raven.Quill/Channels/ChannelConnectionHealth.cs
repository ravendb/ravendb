namespace Raven.Quill.Channels;

internal sealed record ChannelConnectionSnapshot(
    bool IsConnected,
    DateTime? LastConnectedAt,
    string? LastConnectionError,
    DateTime? LastInboundAt,
    DateTime? LastSendErrorAt,
    string? LastSendError);

internal sealed class ChannelConnectionHealth
{
    private readonly object _sync = new();

    private bool _isConnected;
    private DateTime? _lastConnectedAt;
    private string? _lastConnectionError;
    private DateTime? _lastInboundAt;
    private DateTime? _lastSendErrorAt;
    private string? _lastSendError;

    public static ChannelConnectionHealth Continuing(ChannelConnectionHealth previous)
    {
        var snapshot = previous.Snapshot();
        return new ChannelConnectionHealth
        {
            _lastConnectedAt = snapshot.LastConnectedAt,
            _lastConnectionError = snapshot.LastConnectionError,
            _lastInboundAt = snapshot.LastInboundAt,
            _lastSendErrorAt = snapshot.LastSendErrorAt,
            _lastSendError = snapshot.LastSendError,
        };
    }

    public bool IsConnected
    {
        get { lock (_sync) return _isConnected; }
    }

    public ChannelConnectionSnapshot Snapshot()
    {
        lock (_sync)
            return new ChannelConnectionSnapshot(
                _isConnected, _lastConnectedAt, _lastConnectionError, _lastInboundAt, _lastSendErrorAt, _lastSendError);
    }

    public void MarkConnected()
    {
        lock (_sync)
        {
            _isConnected = true;
            _lastConnectedAt = DateTime.UtcNow;
            _lastConnectionError = null;
        }
    }

    public void MarkDisconnected(string? error)
    {
        lock (_sync)
        {
            _isConnected = false;
            if (error is not null)
                _lastConnectionError = error;
        }
    }

    public void MarkReceived()
    {
        lock (_sync)
            _lastInboundAt = DateTime.UtcNow;
    }

    public void MarkSendFailed(string error)
    {
        lock (_sync)
        {
            _lastSendErrorAt = DateTime.UtcNow;
            _lastSendError = error;
        }
    }

    public void MarkSendSucceeded()
    {
        lock (_sync)
        {
            _lastSendErrorAt = null;
            _lastSendError = null;
        }
    }
}
