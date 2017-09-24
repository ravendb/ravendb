using System.Collections.Generic;

namespace Raven.Client.Documents.Smuggler
{
    public class DatabaseSmugglerExportOptions : DatabaseSmugglerOptions, IDatabaseSmugglerExportOptions
    {
        public DatabaseSmugglerExportOptions()
        {
            Collections = new List<string>();
        }

        public List<string> Collections { get; set; }
    }

    internal interface IDatabaseSmugglerExportOptions : IDatabaseSmugglerOptions
    {
        List<string> Collections { get; set; }
    }
}
