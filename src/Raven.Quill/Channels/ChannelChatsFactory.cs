using Microsoft.Extensions.Options;
using Raven.Client.Documents;
using Raven.Quill.Agents;
using Raven.Quill.Hosting;
using Sparrow.Logging;

namespace Raven.Quill.Channels;

internal sealed class ChannelChatsFactory(IDocumentStore store, IAgentRouter router, IOptions<ApplianceOptions> options)
{
    public ChannelChats<TMessage> Create<TMessage>(
        string database, Channel channel, IPlatformTurns<TMessage> platform, ChannelConnectionHealth? health,
        IRavenLogger logger)
        where TMessage : IChannelMessage =>
        new(channel,
            new ChannelTurns<TMessage>(platform, database, channel, health, store, router, options.Value, logger),
            options.Value, logger);
}
