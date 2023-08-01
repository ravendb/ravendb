namespace Raven.Client.Documents.Smuggler
{
    public sealed class DatabaseSmugglerExportOptions : DatabaseSmugglerOptions, IDatabaseSmugglerExportOptions
    {
    }

    internal interface IDatabaseSmugglerExportOptions : IDatabaseSmugglerOptions
    {
    }
}
