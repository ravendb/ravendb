using System.Net;
using Microsoft.Extensions.Logging.Abstractions;
using Raven.Quill.Channels;
using Raven.Quill.Discord;
using Raven.Quill.Hosting;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests;

public class ChannelStreamingReplyTests
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

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Discord_preview_backs_off_for_retry_after_and_still_finalizes()
    {
        var client = new RateLimitingDiscordClient();
        var options = new DiscordOptions { EditDebounce = TimeSpan.Zero };
        var reply = new DiscordStreamingReply(
            client, "bot-token", "dm-channel", options, NullLogger.Instance, CancellationToken.None);

        await reply.OnChunkAsync("a");

        client.NextEditRateLimited = TimeSpan.FromMinutes(5);
        await reply.OnChunkAsync("b");
        await reply.OnChunkAsync("c");
        await reply.OnChunkAsync("d");

        Assert.Equal(1, client.CreateCalls);
        Assert.Equal(1, client.EditCalls);

        await reply.FinalizeAsync();

        Assert.Equal(2, client.EditCalls);
        Assert.Equal("abcd", client.LastEditContent);
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

    private sealed class RateLimitingDiscordClient : IDiscordClient
    {
        public int CreateCalls { get; private set; }
        public int EditCalls { get; private set; }
        public string LastEditContent { get; private set; } = "";
        public TimeSpan? NextEditRateLimited { get; set; }

        public Task<string> CreateMessageAsync(string botToken, string channelId, string content, CancellationToken ct)
        {
            CreateCalls++;
            return Task.FromResult("message-1");
        }

        public Task EditMessageAsync(
            string botToken, string channelId, string messageId, string content, CancellationToken ct)
        {
            EditCalls++;
            if (NextEditRateLimited is { } retryAfter)
            {
                NextEditRateLimited = null;
                throw new DiscordApiException("rate limited", HttpStatusCode.TooManyRequests, retryAfter);
            }

            LastEditContent = content;
            return Task.CompletedTask;
        }

        public Task<(DiscordBotIdentity? Identity, string? Error, bool DiscordResponded)> GetBotIdentityAsync(
            string botToken, CancellationToken ct) => throw new NotSupportedException();

        public Task<string> GetGatewayUrlAsync(string botToken, CancellationToken ct) =>
            throw new NotSupportedException();
    }
}
