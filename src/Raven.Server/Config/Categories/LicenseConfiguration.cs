using System.ComponentModel;
using Raven.Server.Config.Attributes;

namespace Raven.Server.Config.Categories
{
    public class LicenseConfiguration : ConfigurationCategory
    {
        // these are actually ServerWideOnly, but we want to support configuring this via env variables, in which
        // case it leaks to the database level and throws

        [Description("The full license string for RavenDB. If License is specified, it overrides the License.Path configuration.")]
        [DefaultValue(null)]
        [ConfigurationEntry("License", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public string License { get; set; }

        [Description("The path to the license file for RavenDB, default for ~\\license.json")]
        [DefaultValue("~/license.json")]
        [ConfigurationEntry("License.Path", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public string LicensePath { get; set; }

        [Description("EXPERT ONLY. Skip logging of lease license errors")]
        [DefaultValue(false)]
        [ConfigurationEntry("License.SkipLeasingErrorsLogging", ConfigurationEntryScope.ServerWideOnly)]
        public bool SkipLeasingErrorsLogging { get; set; }
    }
}
