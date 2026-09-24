using Raven.Quill.Channels;
using SlackNet;
using SlackNet.Events;

namespace Raven.Quill.Slack;

internal sealed class SlackMessageHandler(
    string database,
    string channelDocId,
    string shortChannelId,
    SlackSettings settings,
    SlackInboundProcessor processor,
    SlackHealthRegistry health) : IEventHandler
{
    public Task Handle(EventCallback callback)
    {
        if (callback.TeamId != settings.TeamId ||
            callback.Event is not MessageEvent { ChannelType: "im" } message ||
            string.IsNullOrEmpty(message.BotId) == false ||
            string.IsNullOrEmpty(message.User) || message.User == settings.BotUserId ||
            string.IsNullOrEmpty(message.Channel))
            return Task.CompletedTask;

        var kind = message switch
        {
            SlackNet.Events.FileShare => "unsupported",
            { Subtype: null or "" } => "text",
            _ => null,
        };
        if (kind is null)
            return Task.CompletedTask;

        health.RecordInbound(database, shortChannelId);
        processor.Enqueue(
            database, channelDocId, message.User, message.Channel, callback.EventId ?? "", kind, message.Text);
        return Task.CompletedTask;
    }
}
