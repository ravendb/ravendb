using System.Text.Json.Serialization;

namespace Raven.Quill.Wizard;

public sealed class WizardError
{
    public WizardError()
    {
    }

    public WizardError(string message, string? details = null)
    {
        Message = message;
        Details = details;
    }

    [JsonRequired]
    public string Message { get; set; } = string.Empty;

    public string? Details { get; set; }
}
