using System.ComponentModel;
using Raven.Server.Config.Attributes;

namespace Raven.Server.Config.Categories
{
    public class LicenseConfiguration : ConfigurationCategory
    {
        [Description("The full license string for RavenDB. If License is specified, it overrides the License.Path configuration.")]
        [DefaultValue(null)]
        [ConfigurationEntry("License", ConfigurationEntryScope.ServerWideOnly)]
        public string License { get; set; }

        [Description("The path to the license file for RavenDB, default for ~\\license.xml")]
        [DefaultValue(null)]
        [ConfigurationEntry("License.Path", ConfigurationEntryScope.ServerWideOnly)]
        public string LicensePath { get; set; }
    }
}
