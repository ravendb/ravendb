using Microsoft.Extensions.Options;
using Raven.Quill.Channels;
using Raven.Quill.Hosting;

namespace Raven.Quill.Slack;

internal static class SlackChannelRegistration
{
    public static IServiceCollection AddSlackChannel(this IServiceCollection services)
    {
        services.AddOptions<ApplianceOptions>()
            .Configure(o => EnvironmentVariables.Read(Constants.Configuration.SlackApiUrl, v => o.Slack.ApiUrl = v))
            .Validate(o => Uri.TryCreate(o.Slack.ApiUrl, UriKind.Absolute, out var u) &&
                           (u.Scheme == Uri.UriSchemeHttp || u.Scheme == Uri.UriSchemeHttps),
                "Slack ApiUrl must be an absolute http(s) URL")
            .Validate(o => o.Slack.RequestTimeout > TimeSpan.Zero, "Slack RequestTimeout must be positive")
            .Validate(o => o.Slack.MessageLimit > 0 &&
                           o.Slack.MessageLimit <= SlackOptions.MarkdownBlockLimit &&
                           o.Slack.MessageLimit <= SlackOptions.ApiMessageLimit / SlackApiClient.MaxEscapeExpansion,
                $"Slack MessageLimit must be between 1 and {Math.Min(SlackOptions.MarkdownBlockLimit, SlackOptions.ApiMessageLimit / SlackApiClient.MaxEscapeExpansion)}, " +
                "so the markdown block and the worst-case escaped fallback text both stay within Slack's caps")
            .Validate(o => o.Slack.EditDebounce > TimeSpan.Zero, "Slack EditDebounce must be positive")
            .Validate(o => o.Slack.SocketRestartDelay > TimeSpan.Zero, "Slack SocketRestartDelay must be positive");

        services.AddHttpClient(SlackSdk.HttpClientName, static (sp, http) =>
        {
            http.Timeout = sp.GetRequiredService<IOptions<ApplianceOptions>>().Value.Slack.RequestTimeout;
        });
        services.AddSingleton<SlackSdk>();
        services.AddTransient<ISlackClient, SlackApiClient>();
        services.AddSingleton<SlackUserDirectory>();

        return services.AddChannelProvider<SlackRuntimeFactory, SlackTurns, SlackMessage, SlackBot>();
    }
}
