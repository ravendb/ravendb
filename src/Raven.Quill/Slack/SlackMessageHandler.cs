using Raven.Quill.Channels;
using SlackNet;
using SlackNet.Events;
using Channel = Raven.Quill.Channels.Channel;

namespace Raven.Quill.Slack;

internal sealed class SlackMessageHandler(
    Channel channel,
    ChannelChats<SlackMessage> chats,
    ChannelConnectionHealth health) : IEventHandler
{
    public Task Handle(EventCallback callback)
    {
        var settings = channel.Slack!;

        if (callback.TeamId != settings.TeamId ||
            callback.Event is not MessageEvent { ChannelType: "im" } message ||
            string.IsNullOrEmpty(message.BotId) == false ||
            string.IsNullOrEmpty(message.User) || message.User == settings.BotUserId ||
            string.IsNullOrEmpty(message.Channel))
            return Task.CompletedTask;

        ChannelMessageKind? kind = message switch
        {
            SlackNet.Events.FileShare => ChannelMessageKind.Unsupported,
            { Subtype: null or "" } => ChannelMessageKind.Prompt,
            _ => null,
        };
        if (kind is null)
            return Task.CompletedTask;

        health.MarkReceived();
        chats.Enqueue(new SlackMessage(message.User, message.Channel, kind.Value, message.Text));
        return Task.CompletedTask;
    }
}
