using System.ComponentModel;
using Raven.Server.Config.Attributes;

namespace Raven.Server.Config.Categories
{
    public class SecurityConfiguration : ConfigurationCategory
    {
        [Description("Enables or disables the authentication for the server.")]
        [DefaultValue(false)]
        [ConfigurationEntry("Security.Authentication.Enabled")]
        public bool AuthenticationEnabled { get; set; }
        
        [Description("If server is binded to non-localhost e.g. 'http://0.0.0.0:8080' then we require authentication to be enabled unless this setting is set to false.")]
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