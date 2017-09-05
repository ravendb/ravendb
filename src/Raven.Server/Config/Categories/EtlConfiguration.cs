using System.ComponentModel;
using Raven.Server.Config.Attributes;
using Raven.Server.Config.Settings;

namespace Raven.Server.Config.Categories
{
    public class EtlConfiguration : ConfigurationCategory
    {
        [Description("Number of seconds after which SQL command will timeout. Default: null (use provider default). Can be overriden by setting CommandTimeout property value in SQL ETL configuration.")]
        [DefaultValue(typeof(string), null)]
        [TimeUnit(TimeUnit.Seconds)]
        [ConfigurationEntry("ETL.SQL.CommandTimeoutInSec", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public TimeSetting? SqlCommandTimeout { get; set; }

        [Description("Number of seconds after which extraction and transformation will end and loading will start.")]
        [DefaultValue(60 * 5)]
        [TimeUnit(TimeUnit.Seconds)]
        [ConfigurationEntry("ETL.ExtractAndTransformTimeoutInSec", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public TimeSetting ExtractAndTransformTimeout { get; protected set; }

        [Description("Max number of extracted documents in ETL batch")]
        [DefaultValue(null)]
        [ConfigurationEntry("ETL.MaxNumberOfExtractedDocuments", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public int? MaxNumberOfExtractedDocuments { get; protected set; }
    }
}
