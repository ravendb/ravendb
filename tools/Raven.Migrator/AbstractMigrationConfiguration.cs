using System.Collections.Generic;

namespace Raven.Migrator
{
    public class AbstractMigrationConfiguration
    {
        public string Command { get; set; }

        public string DatabaseName { get; set; }

        public bool ConsoleExport { get; set; }

        public string ExportFilePath { get; set; }

        public Dictionary<string, string> CollectionsToMigrate { get; set; }
    }
}
