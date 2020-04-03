using System.ComponentModel;
using Raven.Server.Config.Attributes;

namespace Raven.Server.Config.Categories
{
    [ConfigurationCategory(ConfigurationCategoryType.Subscriptions)]
    public class SubscriptionsConfiguration : ConfigurationCategory
    {
        [DefaultValue(1000)]
        [ConfigurationEntry("Subscriptions.MaxNumberOfConcurrentConnections", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        [Description("Amount of concurrent subscription connections per database")]
        public int MaxNumberOfConcurrentConnections { get; set; }
    }
}
