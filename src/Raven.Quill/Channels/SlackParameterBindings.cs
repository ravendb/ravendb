using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Quill.Slack;

namespace Raven.Quill.Channels;

internal static class SlackParameterBindings
{
    internal readonly record struct BindResult(Dictionary<string, string>? Parameters, string? Error);

    internal static async Task<BindResult> BindAsync(
        AiAgentConfiguration config,
        Dictionary<string, ChannelParameterBinding> channelBindings,
        string senderUserId,
        Func<Task<SlackUserInfo>> lookupSender)
    {
        var bound = new Dictionary<string, string>();
        SlackUserInfo? sender = null;

        foreach (var (name, binding) in channelBindings)
        {
            switch (binding.Source)
            {
                case ChannelParameterSource.Constant:
                    bound[name] = binding.Value ?? "";
                    break;

                case ChannelParameterSource.UserId:
                    bound[name] = senderUserId;
                    break;

                case ChannelParameterSource.Email:
                    if (sender is null)
                    {
                        try
                        {
                            sender = await lookupSender();
                        }
                        catch (SlackApiException e)
                        {
                            return new BindResult(null, e.Error == SlackApiException.MissingScopeError
                                ? $"parameter '{name}': reading the sender's email needs the users:read and " +
                                  "users:read.email scopes; add them to the Slack app and reinstall it to the workspace"
                                : $"parameter '{name}': could not read the sender's email from Slack: {e.Message}");
                        }
                    }

                    if (sender.Email is null)
                        return new BindResult(null,
                            $"parameter '{name}': Slack user {senderUserId} has no email on their profile");

                    bound[name] = sender.Email;
                    break;

                default:
                    return new BindResult(null, $"parameter '{name}' has an unsupported binding source {binding.Source}");
            }
        }

        var unbound = (config.Parameters ?? [])
            .Select(parameter => parameter.Name)
            .Where(name => string.IsNullOrWhiteSpace(name) == false && bound.ContainsKey(name) == false)
            .ToArray();
        if (unbound.Length > 0)
            return new BindResult(null, $"agent '{config.Identifier}' has unbound parameter(s): {string.Join(", ", unbound)}");

        return new BindResult(bound, null);
    }
}
