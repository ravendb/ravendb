namespace Raven.Quill.Wizard;

public sealed record ConnectRequest(
    string Provider,
    string ConnectionString);
