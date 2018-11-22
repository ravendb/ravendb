using System;
using System.ComponentModel;
using System.Linq;
using Microsoft.Extensions.Configuration;
using Raven.Server.Config.Attributes;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.ServerWide;

namespace Raven.Server.Config.Categories
{
    public class StudioConfiguration : ConfigurationCategory
    {
        [Description("The directory in which RavenDB will search the studio files, defaults to the base directory")]
        [DefaultValue(null)]
        [ConfigurationEntry("Studio.Path", ConfigurationEntryScope.ServerWideOnly)]
        public string Path { get; set; }
    }
}
