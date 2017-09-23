using System.Collections.Generic;

namespace Raven.Client.Documents.Smuggler
{
    public class DatabaseSmugglerExportOptions : DatabaseSmugglerOptions, IDatabaseSmugglerExportOptions
    {
        public DatabaseSmugglerExportOptions()
        {
            CollectionsToExport = new List<string>();
        }

        public List<string> CollectionsToExport { get; set; }
    }

    internal interface IDatabaseSmugglerExportOptions : IDatabaseSmugglerOptions
    {
        List<string> CollectionsToExport { get; set; }
    }
}
