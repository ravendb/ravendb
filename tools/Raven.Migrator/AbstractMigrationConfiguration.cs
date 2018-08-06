using System.Collections.Generic;

namespace Raven.Migrator
{
    public class AbstractMigrationConfiguration
    {
        public string Command { get; set; }

        public string DatabaseName { get; set; }

        public bool ConsoleExport { get; set; }

        public string ExportFilePath { get; set; }

        public List<Collection> CollectionsToMigrate { get; set; }
    }

    public class Collection
    {
        public string Name { get; set; }

        public string NewName { get; set; }
    }
}
