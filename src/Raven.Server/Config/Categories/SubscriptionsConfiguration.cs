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
        [Description("The maximum number of concurrent subscription connections allowed per database.")]
        public int MaxNumberOfConcurrentConnections { get; set; }
        
        [DefaultValue(ArchivedDataProcessingBehavior.ExcludeArchived)]
        [ConfigurationEntry("Subscriptions.ArchivedDataProcessingBehavior", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        [Description("The default processing behavior for archived documents in a subscription query.")]
        public ArchivedDataProcessingBehavior ArchivedDataProcessingBehavior { get; set; }
    }
}
