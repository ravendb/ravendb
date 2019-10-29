using System;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;

namespace Raven.Server.Documents
{
    public class DatabaseMetricCacher : MetricCacher
    {
        private readonly DocumentDatabase _database;

        public DatabaseMetricCacher(DocumentDatabase database)
        {
            _database = database;
        }

        public void Initialize()
        {
            //Register(MetricCacher.Keys.Database.CountOfAttachments, TimeSpan.FromSeconds(5), GetCountOfAttachments);
        }
    }
}
