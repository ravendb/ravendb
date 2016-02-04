using System.ComponentModel;
using Raven.Database.Config.Attributes;
using Raven.Database.Config.Settings;

namespace Raven.Database.Config.Categories
{
    public class BulkInsertConfiguration : ConfigurationCategory
    {
        [DefaultValue(60000)]
        [TimeUnit(TimeUnit.Milliseconds)]
        [ConfigurationEntry("Raven/BulkImport/BatchTimeoutInMs")]
        [ConfigurationEntry("Raven/BulkImport/BatchTimeout")]
        public TimeSetting ImportBatchTimeout { get; set; }
    }
}