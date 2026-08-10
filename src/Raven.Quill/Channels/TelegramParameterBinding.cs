using Raven.Client.Documents.Operations.AI.Agents;

namespace Raven.Quill.Channels;

public enum TelegramParameterSource
{
    Constant,
    UserId,
    Username,
    PhoneNumber,
}

public sealed class TelegramParameterBinding
{
    public TelegramParameterSource Source { get; set; }

    public string? Value { get; set; }
}

internal static class TelegramParameterBindings
{
    /// Keys of the returned dictionary carry the casing the agent declares, whatever casing was supplied.
    internal static bool TryResolve(
        AiAgentConfiguration config,
        Dictionary<string, TelegramParameterBinding>? supplied,
        out Dictionary<string, TelegramParameterBinding> bindings,
        out string? error)
    {
        bindings = new Dictionary<string, TelegramParameterBinding>();
        error = null;

        var declared = (config.Parameters ?? [])
            .Select(parameter => parameter.Name)
            .Where(name => string.IsNullOrWhiteSpace(name) == false)
            .ToArray();

        var suppliedByName = new Dictionary<string, TelegramParameterBinding>(StringComparer.OrdinalIgnoreCase);
        foreach (var (name, binding) in supplied ?? new Dictionary<string, TelegramParameterBinding>())
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

            if (binding.Source == TelegramParameterSource.Constant)
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
}
