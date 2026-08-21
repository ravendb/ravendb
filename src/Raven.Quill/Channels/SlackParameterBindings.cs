using Raven.Client.Documents.Operations.AI.Agents;

namespace Raven.Quill.Channels;

internal static class SlackParameterBindings
{
    internal static bool IsSupportedSource(ChannelParameterSource source) =>
        source is ChannelParameterSource.Constant or ChannelParameterSource.UserId;

    internal static bool TryResolve(
        AiAgentConfiguration config,
        Dictionary<string, ChannelParameterBinding>? supplied,
        out Dictionary<string, ChannelParameterBinding> bindings,
        out string? error)
    {
        if (ChannelParameterBindings.TryResolve(config, supplied, out bindings, out error) == false)
            return false;

        foreach (var (name, binding) in bindings)
        {
            if (IsSupportedSource(binding.Source))
                continue;

            error = $"parameter binding for '{name}': Slack channels cannot bind {binding.Source}; " +
                    $"use {nameof(ChannelParameterSource.Constant)} or {nameof(ChannelParameterSource.UserId)}";
            bindings = new Dictionary<string, ChannelParameterBinding>();
            return false;
        }

        return true;
    }

    internal static bool TryBind(
        AiAgentConfiguration config,
        Dictionary<string, ChannelParameterBinding> channelBindings,
        string senderUserId,
        out Dictionary<string, string> parameters,
        out string? error)
    {
        var bound = new Dictionary<string, string>();
        parameters = bound;
        error = null;

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

                default:
                    error = $"parameter '{name}' has an unsupported binding source {binding.Source}";
                    return false;
            }
        }

        var unbound = (config.Parameters ?? [])
            .Select(parameter => parameter.Name)
            .Where(name => string.IsNullOrWhiteSpace(name) == false && bound.ContainsKey(name) == false)
            .ToArray();
        if (unbound.Length > 0)
        {
            error = $"agent '{config.Identifier}' has unbound parameter(s): {string.Join(", ", unbound)}";
            return false;
        }

        return true;
    }
}
