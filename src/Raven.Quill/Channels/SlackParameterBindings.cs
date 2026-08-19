using Raven.Client.Documents.Operations.AI.Agents;

namespace Raven.Quill.Channels;

internal static class SlackParameterBindings
{
    internal static bool IsSupportedSource(TelegramParameterSource source) =>
        source is TelegramParameterSource.Constant or TelegramParameterSource.UserId;

    internal static bool TryResolve(
        AiAgentConfiguration config,
        Dictionary<string, TelegramParameterBinding>? supplied,
        out Dictionary<string, TelegramParameterBinding> bindings,
        out string? error)
    {
        if (TelegramParameterBindings.TryResolve(config, supplied, out bindings, out error) == false)
            return false;

        foreach (var (name, binding) in bindings)
        {
            if (IsSupportedSource(binding.Source))
                continue;

            error = $"parameter binding for '{name}': Slack channels cannot bind {binding.Source}; " +
                    $"use {nameof(TelegramParameterSource.Constant)} or {nameof(TelegramParameterSource.UserId)}";
            bindings = new Dictionary<string, TelegramParameterBinding>();
            return false;
        }

        return true;
    }

    internal static bool TryBind(
        AiAgentConfiguration config,
        Dictionary<string, TelegramParameterBinding> channelBindings,
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
                case TelegramParameterSource.Constant:
                    bound[name] = binding.Value ?? "";
                    break;

                case TelegramParameterSource.UserId:
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
