using Raven.Client.Documents.Operations.AI.Agents;

namespace Raven.Quill.Channels;

internal static class DiscordParameterBindings
{
    internal readonly record struct BindResult(Dictionary<string, string>? Parameters, string? Error);

    internal static BindResult Bind(
        AiAgentConfiguration config,
        Dictionary<string, ChannelParameterBinding> channelBindings,
        string senderUserId,
        string? senderUsername)
    {
        var bound = new Dictionary<string, string>();

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

                case ChannelParameterSource.Username:
                    if (string.IsNullOrEmpty(senderUsername))
                        return new BindResult(null,
                            $"parameter '{name}': Discord user {senderUserId} has no username");

                    bound[name] = senderUsername;
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
