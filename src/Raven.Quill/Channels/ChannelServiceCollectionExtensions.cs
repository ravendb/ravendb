using Microsoft.Extensions.Options;
using Raven.Client.Documents;
using Raven.Quill.Agents;
using Raven.Quill.Hosting;

using Raven.Quill.Logging;

namespace Raven.Quill.Channels;

internal static class ChannelServiceCollectionExtensions
{
    public static IServiceCollection AddChannels(this IServiceCollection services)
    {
        services.AddOptions<ApplianceOptions>()
            .Validate(o => o.ChannelApplyChangesInterval > TimeSpan.Zero, "ChannelApplyChangesInterval must be positive")
            .Validate(o => o.ChannelSenderQueueCapacity > 0, "ChannelSenderQueueCapacity must be positive")
            .Validate(o => o.ChannelSenderIdleTimeout > TimeSpan.Zero, "ChannelSenderIdleTimeout must be positive");

        return services.AddSingleton<ChannelManager>();
    }

    public static IServiceCollection AddChannelProvider<TFactory, TTurns, TMessage, TBot>(
        this IServiceCollection services)
        where TFactory : class, IChannelRuntimeFactory
        where TTurns : class, IPlatformTurns<TMessage, TBot>
        where TMessage : IChannelMessage
        where TBot : class, IChannelBot =>
        services
            .AddSingleton<TTurns>()
            .AddSingleton(sp => new ChannelTurns<TMessage, TBot>(
                sp.GetRequiredService<TTurns>(), sp.GetRequiredService<IDocumentStore>(),
                sp.GetRequiredService<IAgentRouter>(), sp.GetRequiredService<IOptions<ApplianceOptions>>(),
                sp.GetRequiredService<QuillLogger<TTurns>>().RavenLogger))
            .AddSingleton(sp =>
            {
                var o = sp.GetRequiredService<IOptions<ApplianceOptions>>().Value;
                return new ChannelChats<TMessage>(
                    sp.GetRequiredService<ChannelTurns<TMessage, TBot>>(),
                    sp.GetRequiredService<QuillLogger<TTurns>>().RavenLogger,
                    o.ChannelSenderQueueCapacity, o.ChannelSenderIdleTimeout);
            })
            .AddHostedService(sp => sp.GetRequiredService<ChannelChats<TMessage>>())
            .AddSingleton<IChannelRuntimeFactory, TFactory>();
}
