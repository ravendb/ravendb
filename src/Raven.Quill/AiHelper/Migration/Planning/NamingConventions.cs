namespace Raven.Quill.AiHelper.Migration.Planning;

public sealed record NamingConventions(
    PropertyCase PropertyCase = PropertyCase.Unspecified,
    string? PropertyLanguage = null,
    string? Notes = null);
