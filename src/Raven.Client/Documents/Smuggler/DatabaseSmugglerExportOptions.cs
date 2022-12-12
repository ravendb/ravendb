namespace Raven.Client.Documents.Smuggler
{
    public class DatabaseSmugglerExportOptions : DatabaseSmugglerOptions, IDatabaseSmugglerExportOptions
    {
    }

    internal interface IDatabaseSmugglerExportOptions : IDatabaseSmugglerOptions
    {
    }
}
