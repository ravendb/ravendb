using Raven.Quill.Hosting;

namespace Raven.Quill.Channels;

internal static class ChannelServiceCollectionExtensions
{
    public static IServiceCollection AddChannels(this IServiceCollection services)
    {
        services.AddOptions<ApplianceOptions>()
            .Validate(o => o.ChannelApplyChangesInterval > TimeSpan.Zero, "ChannelApplyChangesInterval must be positive")
            .Validate(o => o.ChannelSenderQueueCapacity > 0, "ChannelSenderQueueCapacity must be positive")
            .Validate(o => o.ChannelSenderIdleTimeout > TimeSpan.Zero, "ChannelSenderIdleTimeout must be positive")
            .Validate(o => o.ChannelRuntimeStopTimeout > TimeSpan.Zero, "ChannelRuntimeStopTimeout must be positive");

        return services
            .AddSingleton<ChannelChatsFactory>()
            .AddSingleton<ChannelManager>();
    }
}
