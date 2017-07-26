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
        [ConfigurationEntry("Security.Certificate:Path")]
        public string CertificatePath { get; set; }

        [Description("The (optional) password of the .pfx certificate file.")]
        [DefaultValue(null)]
        [ConfigurationEntry("Security.Certificate:Password")]
        public string CertificatePassword { get; set; }

        [Description("A command or executable to run, which will provide a .pfx certificate file. If specified, RavenDB will use HTTPS / SSL for all network activities.")]
        [DefaultValue(null)]
        [ConfigurationEntry("Security.Certificate:Exec")]
        public string CertificateExec { get; set; }

        [Description("The command line arguments for the 'Security.Certificate:Exec' command or executable.")]
        [DefaultValue(null)]
        [ConfigurationEntry("Security.Certificate:Arguments")]
        public string CertificateExecArguments { get; set; }

        [Description("The number of milliseconds to wait for the certificate executable to exit. Default: 30 seconds")]
        [DefaultValue(30000)]
        [ConfigurationEntry("Security.Certificate:Timeout")]
        public int CertificateExecTimeout { get; set; }

        [Description("The path of the (512-bit) Master Key. If specified, RavenDB will use this key to protect secrets.")]
        [DefaultValue(null)]
        [ConfigurationEntry("Security.MasterKey:Path")]
        public string MasterKeyPath { get; set; }

        [Description("A command or executable to run which will provide a (512-bit) Master Key, If specified, RavenDB will use this key to protect secrets.")]
        [DefaultValue(null)]
        [ConfigurationEntry("Security.MasterKey:Exec")]
        public string MasterKeyExec { get; set; }

        [Description("The command line arguments for the 'Security.MasterKey:Exec' command or executable.")]
        [DefaultValue(null)]
        [ConfigurationEntry("Security.MasterKey:Arguments")]
        public string MasterKeyExecArguments { get; set; }

        [Description("The number of milliseconds to wait for the Master Key executable to exit. Default: 30 seconds")]
        [DefaultValue(30000)]
        [ConfigurationEntry("Security.MasterKey:Timeout")]
        public int MasterKeyExecTimeout { get; set; }
    }
}