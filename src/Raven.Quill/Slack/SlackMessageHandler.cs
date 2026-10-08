using Raven.Quill.Channels;
using SlackNet;
using SlackNet.Events;
using Channel = Raven.Quill.Channels.Channel;

namespace Raven.Quill.Slack;

internal sealed class SlackMessageHandler(
    string database,
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

        bool? unsupported = message switch
        {
            SlackNet.Events.FileShare => true,
            { Subtype: null or "" } => false,
            _ => null,
        };
        if (unsupported is null)
            return Task.CompletedTask;

        health.MarkReceived();
        chats.Enqueue(
            callback.EventId ?? "",
            new SlackMessage(database, channel, health, message.User, message.Channel, unsupported.Value, message.Text));
        return Task.CompletedTask;
    }
}
