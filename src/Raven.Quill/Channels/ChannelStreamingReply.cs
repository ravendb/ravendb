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

    protected virtual bool CanPreview => true;

    protected abstract Task ShowPreviewAsync(string text, CancellationToken token);

    protected abstract Task SendFinalAsync(string text, CancellationToken token);

    protected abstract Task EditFinalAsync(string text, CancellationToken token);

    protected abstract void CloseCurrentMessage();

    protected abstract void LogFlushFailure(Exception error);

    public async ValueTask OnChunkAsync(string chunk, CancellationToken token)
    {
        _buffer.Append(chunk);

        if (PendingLength <= messageLimit && DateTime.UtcNow - _lastFlushAt < editDebounce)
            return;

        try
        {
            await FlushPreviewAsync(token);
        }
        catch (Exception e) when (token.IsCancellationRequested == false)
        {
            LogFlushFailure(e);
        }
        finally
        {
            _lastFlushAt = DateTime.UtcNow;
        }
    }

    public async Task FinalizeAsync(CancellationToken token)
    {
        var pending = _buffer.ToString(_flushedUpTo, PendingLength);
        if (string.IsNullOrWhiteSpace(pending))
            return;

        var parts = MessageSplitter.Split(pending, messageLimit);
        for (var i = 0; i < parts.Count; i++)
        {
            if (i == 0 && HasOpenMessage)
                await EditFinalAsync(parts[i], token);
            else
                await SendFinalAsync(parts[i], token);
        }
    }

    private async Task FlushPreviewAsync(CancellationToken token)
    {
        while (PendingLength > messageLimit)
        {
            var pending = _buffer.ToString(_flushedUpTo, PendingLength);
            var cut = MessageSplitter.CutPoint(pending, messageLimit);

            var segment = pending[..cut].TrimEnd();
            if (segment.Length > 0)
            {
                if (HasOpenMessage)
                    await EditFinalAsync(segment, token);
                else
                    await SendFinalAsync(segment, token);

                CloseCurrentMessage();
                LastShownText = "";
            }

            _flushedUpTo += cut;
            while (PendingLength > 0 && char.IsWhiteSpace(_buffer[_flushedUpTo]))
                _flushedUpTo++;
        }

        var preview = _buffer.ToString(_flushedUpTo, PendingLength);
        if (preview.Length == 0 || preview == LastShownText || CanPreview == false)
            return;

        await ShowPreviewAsync(preview, token);
        LastShownText = preview;
    }
}
