namespace Raven.Quill.Wizard;

internal sealed class App
{
    public string? Id { get; set; }

    public string Slug { get; set; } = "";

    public string AppName { get; set; } = "";

    public string Database { get; set; } = "";

    public string CdcTaskName { get; set; } = "";

    public DateTime CreatedAt { get; set; }
}
