using System.ComponentModel;
using Raven.Server.Config.Attributes;

namespace Raven.Server.Config.Categories
{
    public class SecurityConfiguration : ConfigurationCategory
    {
        [Description("TODO")]
        [DefaultValue(false)]
        [ConfigurationEntry("Security.Authentication.Enabled")]
        public bool AuthenticationEnabled { get; set; }

        [DefaultValue(true)]
        [ConfigurationEntry("Security.Authentication.RequiredForPublicNetworks")]
        public bool AuthenticationRequiredForPublicNetworks { get; set; }

        [Description("The path of the .pfx certificate file. If specified, RavenDB will use HTTPS / SSL for all network activities. You can use the ~/ prefix to refer to RavenDB's base directory.")]
        [DefaultValue(null)]
        [ConfigurationEntry("Security.CertificatePath")]
        public string CertificatePath { get; set; }

        [Description("The (optional) password of the .pfx certificate file.")]
        [DefaultValue(null)]
        [ConfigurationEntry("Security.CertificatePassword")]
        public string CertificatePassword { get; set; }
    }
}