using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Text;
using Raven.Server.Config.Attributes;
using Raven.Server.Config.Settings;

namespace Raven.Server.Config.Categories
{
    public class SubscriptionConfiguration : ConfigurationCategory
    {
        [DefaultValue(10)]
        [TimeUnit(TimeUnit.Minutes)]
        [ConfigurationEntry("Subscriptions.ConcurrentConnections")]
        [Description("Amount of concurrent subscription connections.")]
        public int ConcurrentConnections { get; set; }
    }
}
