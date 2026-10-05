using FastTests;
using Raven.Quill.Channels;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests;

public class ChannelStreamingReplyTests(ITestOutputHelper output) : NoDisposalNeeded(output)
{
    [RavenFact(RavenTestCategory.Quill)]
    public async Task Whitespace_only_cut_segments_are_skipped_and_the_stream_recovers()
    {
        var reply = new RecordingReply(messageLimit: 10);

        await reply.OnChunkAsync(new string('\n', 15));
        await reply.OnChunkAsync("hello");
        await reply.FinalizeAsync();

        Assert.Equal(0, reply.EmptyAttempts);
        Assert.Equal("hello", Assert.Single(reply.Previews));
        Assert.Equal("hello", Assert.Single(reply.Finals));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Overflow_still_flushes_the_full_segment_before_the_next_preview()
    {
        var reply = new RecordingReply(messageLimit: 10);

        await reply.OnChunkAsync("0123456789");
        await reply.OnChunkAsync("x");
        await reply.FinalizeAsync();

        Assert.Equal(0, reply.EmptyAttempts);
        Assert.Equal(["0123456789", "x"], reply.Previews);
        Assert.Contains("0123456789", reply.Finals);
        Assert.Contains("x", reply.Finals);
    }

    private sealed class RecordingReply(int messageLimit)
        : ChannelStreamingReply(messageLimit, TimeSpan.Zero)
    {
        private bool _open;

        public List<string> Previews { get; } = [];
        public List<string> Finals { get; } = [];
        public int EmptyAttempts { get; private set; }

        protected override bool HasOpenMessage => _open;

        protected override Task ShowPreviewAsync(string text)
        {
            Previews.Add(text);
            _open = true;
            return Task.CompletedTask;
        }

        protected override Task SendFinalAsync(string text) => RecordFinal(text);

        protected override Task EditFinalAsync(string text) => RecordFinal(text);

        protected override void CloseCurrentMessage() => _open = false;

        protected override void LogFlushFailure(Exception error)
        {
        }

        private Task RecordFinal(string text)
        {
            if (text.Length == 0)
            {
                EmptyAttempts++;
                throw new InvalidOperationException("the platform rejects an empty message");
            }

            Finals.Add(text);
            _open = true;
            return Task.CompletedTask;
        }
    }
}
