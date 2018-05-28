using System;
using System.ComponentModel;
using Raven.Server.Config.Attributes;

namespace Raven.Server.Config.Categories
{
    public class EmbeddedConfiguration : ConfigurationCategory
    {
        private int? _parentProcessId;

        [Description("Watch the parent process id and exit when it exited as well (for tests)")]
        [DefaultValue(null)]
        [ConfigurationEntry("Testing.ParentProcessId", ConfigurationEntryScope.ServerWideOnly)]
        public int? LegacyParentProcessId
        {
            get => _parentProcessId;
            set => _parentProcessId = value;
        }

        [Description("Watch the parent process id and exit when it exited as well")]
        [DefaultValue(null)]
        [ConfigurationEntry("Embedded.ParentProcessId", ConfigurationEntryScope.ServerWideOnly)]
        public int? ParentProcessId
        {
            get => _parentProcessId;
            set => _parentProcessId = value;
        }


    }
}
