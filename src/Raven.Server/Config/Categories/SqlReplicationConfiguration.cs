using System.ComponentModel;
using Raven.Server.Config.Attributes;
using Raven.Server.Config.Settings;

namespace Raven.Server.Config.Categories
{
    public class SqlReplicationConfiguration : ConfigurationCategory
    {
        [Description("Number of seconds after which SQL command will timeout. Default: null (use provider default). Can be overriden by setting CommandTimeout property value in SQL Replication configuration.")]
        [DefaultValue(typeof(string), null)]
        [TimeUnit(TimeUnit.Seconds)]
        [ConfigurationEntry("Raven/SqlReplication/CommandTimeoutInSec")]
        public TimeSetting? CommandTimeout { get; set; }
    }
}