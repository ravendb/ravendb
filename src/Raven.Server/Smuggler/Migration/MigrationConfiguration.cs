using System.Collections.Generic;

namespace Raven.Server.Smuggler.Migration
{
    public class SingleDatabaseMigrationConfiguration : MigrationConfigurationBase
    {
        public string DatabaseName { get; set; }
    }

    public class DatabasesMigrationConfiguration : MigrationConfigurationBase
    {
        public DatabasesMigrationConfiguration()
        {
            DatabasesNames = new List<string>();
        }

        public List<string> DatabasesNames { get; set; }
    }

    public abstract class MigrationConfigurationBase
    {
        public MajorVersion MajorVersion { get; set; }

        public string ServerUrl { get; set; }

        public string UserName { get; set; }

        public string Password { get; set; }

        public string Domain { get; set; }
    }
}
