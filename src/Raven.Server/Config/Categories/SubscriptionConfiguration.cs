using System.ComponentModel;
using Raven.Server.Config.Attributes;

namespace Raven.Server.Config.Categories
{
    public class SubscriptionConfiguration : ConfigurationCategory
    {
        [DefaultValue(1000)]
        [ConfigurationEntry("Subscriptions.MaxNumberOfConcurrentConnections", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        [Description("Amount of concurrent subscription connections per database")]
        public int MaxNumberOfConcurrentConnections { get; set; }
    }
}
