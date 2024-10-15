using System.ComponentModel;
using Raven.Client.Documents.DataArchival;
using Raven.Server.Config.Attributes;

namespace Raven.Server.Config.Categories
{
    [ConfigurationCategory(ConfigurationCategoryType.Subscriptions)]
    public sealed class SubscriptionsConfiguration : ConfigurationCategory
    {
        [DefaultValue(1000)]
        [ConfigurationEntry("Subscriptions.MaxNumberOfConcurrentConnections", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        [Description("Amount of concurrent subscription connections per database")]
        public int MaxNumberOfConcurrentConnections { get; set; }
        
        [DefaultValue(ArchivedDataProcessingBehavior.ExcludeArchived)]
        [ConfigurationEntry("Subscriptions.ArchivedDataProcessingBehavior", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        [Description("The default subscriptions archived data processing behavior per database")]
        public ArchivedDataProcessingBehavior ArchivedDataProcessingBehavior { get; set; }
    }
}
