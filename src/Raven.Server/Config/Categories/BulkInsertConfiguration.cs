using System.ComponentModel;
using Raven.Server.Config.Attributes;
using Raven.Server.Config.Settings;

namespace Raven.Server.Config.Categories
{
    public class BulkInsertConfiguration : ConfigurationCategory
    {
        [DefaultValue(60000)]
        [TimeUnit(TimeUnit.Milliseconds)]
        [ConfigurationEntry("Raven/BulkImport/BatchTimeoutInMs")]
        [LegacyConfigurationEntry("Raven/BulkImport/BatchTimeout")]
        public TimeSetting ImportBatchTimeout { get; set; }
    }
}