using Raven.Client.Documents.Operations.AI.Agents;

namespace Raven.Quill.Channels;

public enum ChannelParameterSource
{
    Constant,
    UserId,
    Username,
    PhoneNumber,
    Email,
}

public sealed class ChannelParameterBinding
{
    public ChannelParameterSource Source { get; set; }

    public string? Value { get; set; }
}

internal static class ChannelParameterBindings
{
    private static readonly Dictionary<ChannelType, ChannelParameterSource[]> SupportedSources = new()
    {
        [ChannelType.Telegram] =
        [
            ChannelParameterSource.Constant, ChannelParameterSource.UserId,
            ChannelParameterSource.Username, ChannelParameterSource.PhoneNumber,
        ],
        [ChannelType.Slack] =
        [
            ChannelParameterSource.Constant, ChannelParameterSource.UserId, ChannelParameterSource.Email,
        ],
    };

    /// Keys of the returned dictionary carry the casing the agent declares, whatever casing was supplied.
    internal static bool TryResolve(
        AiAgentConfiguration config,
        ChannelType channelType,
        Dictionary<string, ChannelParameterBinding>? supplied,
        out Dictionary<string, ChannelParameterBinding> bindings,
        out string? error)
    {
        var supportedSources = SupportedSources[channelType];
        bindings = new Dictionary<string, ChannelParameterBinding>();
        error = null;

        var declared = (config.Parameters ?? [])
            .Select(parameter => parameter.Name)
            .Where(name => string.IsNullOrWhiteSpace(name) == false)
            .ToArray();

        var suppliedByName = new Dictionary<string, ChannelParameterBinding>(StringComparer.OrdinalIgnoreCase);
        foreach (var (name, binding) in supplied ?? new Dictionary<string, ChannelParameterBinding>())
        {
            if (binding is not null)
                suppliedByName[name] = binding;
        }

        var unknown = suppliedByName.Keys
            .Where(name => declared.Contains(name, StringComparer.OrdinalIgnoreCase) == false)
            .ToArray();
        if (unknown.Length > 0)
        {
            error = $"parameter binding(s) for undeclared agent parameter(s): {string.Join(", ", unknown)}";
            return false;
        }

        var missing = new List<string>();
        foreach (var name in declared)
        {
            if (suppliedByName.TryGetValue(name, out var binding) == false)
            {
                missing.Add(name);
                continue;
            }

            if (supportedSources.Contains(binding.Source) == false)
            {
                error = $"parameter binding for '{name}': {channelType} channels cannot bind {binding.Source}; " +
                        $"use {FormatSources(supportedSources)}";
                return false;
            }

            if (binding.Source == ChannelParameterSource.Constant)
            {
                if (string.IsNullOrWhiteSpace(binding.Value))
                {
                    error = $"parameter binding for '{name}': a Constant binding requires a value";
                    return false;
                }
            }
            else if (string.IsNullOrWhiteSpace(binding.Value) == false)
            {
                error = $"parameter binding for '{name}': a value applies only to Constant bindings";
                return false;
            }

            bindings[name] = binding;
        }

        if (missing.Count > 0)
        {
            error = $"missing parameter binding(s) for agent parameter(s): {string.Join(", ", missing)}";
            return false;
        }

        return true;
    }

    private static string FormatSources(ChannelParameterSource[] sources) =>
        $"{string.Join(", ", sources[..^1])} or {sources[^1]}";
}
