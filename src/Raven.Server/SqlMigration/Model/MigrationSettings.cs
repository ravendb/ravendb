using System.Collections.Generic;

namespace Raven.Server.SqlMigration.Model
{
    public class MigrationSettings
    {
        public bool BinaryToAttachment { get; set; }
        public List<RootCollection> Collections { get; set; }
        public int BatchSize { get; set; } = 1000;
    }
}
