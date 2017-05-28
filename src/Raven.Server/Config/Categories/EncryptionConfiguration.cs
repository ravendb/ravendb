using System.ComponentModel;
using Raven.Server.Config.Attributes;
using Raven.Server.Config.Settings;

namespace Raven.Server.Config.Categories
{
    public class SecurityConfiguration : ConfigurationCategory
    {
        [Description("The path of the .pfx certificate file. If specified, RavenDB will use HTTPS / SSL for all network activities. You can use the ~/ prefix to refer to RavenDB's base directory.")]
        [DefaultValue(null)]
        [ConfigurationEntry("Raven/Certificate/Path")]
        public string CertificateFilePath { get; set; }

        [Description("The (optional) password of the .pfx certificate file.")]
        [DefaultValue(null)]
        [ConfigurationEntry("Raven/Certificate/Password")]
        public string CertificatePassword { get; set; }
    }
}