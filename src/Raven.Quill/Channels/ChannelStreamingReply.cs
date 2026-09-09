using System.Text;

namespace Raven.Quill.Channels;

internal abstract class ChannelStreamingReply(int messageLimit, TimeSpan editDebounce)
{
    private readonly StringBuilder _buffer = new();
    private int _flushedUpTo;
    private DateTime _lastFlushAt;

    private int PendingLength => _buffer.Length - _flushedUpTo;

    public bool IsEmpty => _buffer.Length == 0;

    protected string LastShownText { get; private set; } = "";

    protected abstract bool HasOpenMessage { get; }

    protected abstract Task ShowPreviewAsync(string text);

    protected abstract Task SendFinalAsync(string text);

    protected abstract Task EditFinalAsync(string text);

    protected abstract void CloseCurrentMessage();

    protected abstract void LogFlushFailure(Exception error);

    public async ValueTask OnChunkAsync(string chunk)
    {
        _buffer.Append(chunk);

        if (PendingLength <= messageLimit && DateTime.UtcNow - _lastFlushAt < editDebounce)
            return;

        try
        {
            await FlushPreviewAsync();
        }
        catch (Exception e) when (e is not OperationCanceledException)
        {
            LogFlushFailure(e);
        }
        finally
        {
            _lastFlushAt = DateTime.UtcNow;
        }
    }

    public async Task FinalizeAsync()
    {
        var pending = _buffer.ToString(_flushedUpTo, PendingLength);
        if (string.IsNullOrWhiteSpace(pending))
            return;

        var parts = MessageSplitter.Split(pending, messageLimit);
        for (var i = 0; i < parts.Count; i++)
        {
            if (i == 0 && HasOpenMessage)
                await EditFinalAsync(parts[i]);
            else
                await SendFinalAsync(parts[i]);
        }
    }

    private async Task FlushPreviewAsync()
    {
        while (PendingLength > messageLimit)
        {
            var pending = _buffer.ToString(_flushedUpTo, PendingLength);
            var cut = MessageSplitter.CutPoint(pending, messageLimit);

            var segment = pending[..cut].TrimEnd();
            if (HasOpenMessage)
                await EditFinalAsync(segment);
            else
                await SendFinalAsync(segment);

            _flushedUpTo += cut;
            while (PendingLength > 0 && char.IsWhiteSpace(_buffer[_flushedUpTo]))
                _flushedUpTo++;

            CloseCurrentMessage();
            LastShownText = "";
        }

        var preview = _buffer.ToString(_flushedUpTo, PendingLength);
        if (preview.Length == 0 || preview == LastShownText)
            return;

        await ShowPreviewAsync(preview);
        LastShownText = preview;
    }
}
