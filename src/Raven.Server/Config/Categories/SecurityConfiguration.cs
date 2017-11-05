using System;
using System.ComponentModel;
using System.Linq;
using System.Net;
using Raven.Server.Config.Attributes;
using Raven.Server.Config.Settings;
using Raven.Server.Utils;

namespace Raven.Server.Config.Categories
{
    public class SecurityConfiguration : ConfigurationCategory
    {
        [Description("The .pfx certificate in Base64 format. If specified, RavenDB will use HTTPS/SSL for all network activities. Certificate setting priority order: 1)Base64 2)Path 3)Executable")]
        [DefaultValue(null)]
        [ConfigurationEntry("Security.Certificate.Base64", ConfigurationEntryScope.ServerWideOnly)]
        public string Base64 { get; set; }

        [Description("The path to .pfx certificate file. If specified, RavenDB will use HTTPS/SSL for all network activities. You can use the '~/' prefix to refer to RavenDB's base directory. Certificate setting priority order: 1)Base64 2)Path 3)Executable")]
        [DefaultValue(null)]
        [ConfigurationEntry("Security.Certificate.Path", ConfigurationEntryScope.ServerWideOnly)]
        public string CertificatePath { get; set; }

        [Description("The (optional) password of the .pfx certificate file.")]
        [DefaultValue(null)]
        [ConfigurationEntry("Security.Certificate.Password", ConfigurationEntryScope.ServerWideOnly)]
        public string CertificatePassword { get; set; }

        [Description("A command or executable providing a .pfx certificate file. If specified, RavenDB will use HTTPS/SSL for all network activities. Certificate setting priority order: 1)Base64 2)Path 3)Executable")]
        [DefaultValue(null)]
        [ConfigurationEntry("Security.Certificate.Exec", ConfigurationEntryScope.ServerWideOnly)]
        public string CertificateExec { get; set; }

        [Description("The command line arguments for the 'Security.Certificate.Exec' command or executable.")]
        [DefaultValue(null)]
        [ConfigurationEntry("Security.Certificate.Exec.Arguments", ConfigurationEntryScope.ServerWideOnly)]
        public string CertificateExecArguments { get; set; }

        [Description("The number of seconds to wait for the certificate executable to exit. Default: 30 seconds")]
        [DefaultValue(30)]
        [TimeUnit(TimeUnit.Seconds)]
        [ConfigurationEntry("Security.Certificate.Exec.TimeoutInSec", ConfigurationEntryScope.ServerWideOnly)]
        public TimeSetting CertificateExecTimeout { get; set; }

        [Description("The path of the (512-bit) Master Key. If specified, RavenDB will use this key to protect secrets.")]
        [DefaultValue(null)]
        [ConfigurationEntry("Security.MasterKey.Path", ConfigurationEntryScope.ServerWideOnly)]
        public string MasterKeyPath { get; set; }

        [Description("A command or executable to run which will provide a (512-bit) Master Key, If specified, RavenDB will use this key to protect secrets.")]
        [DefaultValue(null)]
        [ConfigurationEntry("Security.MasterKey.Exec", ConfigurationEntryScope.ServerWideOnly)]
        public string MasterKeyExec { get; set; }

        [Description("The command line arguments for the 'Security.MasterKey.Exec' command or executable.")]
        [DefaultValue(null)]
        [ConfigurationEntry("Security.MasterKey.Exec.Arguments", ConfigurationEntryScope.ServerWideOnly)]
        public string MasterKeyExecArguments { get; set; }

        [Description("The number of milliseconds to wait for the Master Key executable to exit. Default: 30 seconds")]
        [DefaultValue(30)]
        [TimeUnit(TimeUnit.Seconds)]
        [ConfigurationEntry("Security.MasterKey.Exec.TimeoutInSec", ConfigurationEntryScope.ServerWideOnly)]
        public TimeSetting MasterKeyExecTimeout { get; set; }

        [Description("If authentication is disabled, set address range type for which server access is unsecured (None | Local | PrivateNetwork | PublicNetwork).")]
        [DefaultValue(UnsecuredAccessAddressRange.Local)]
        [ConfigurationEntry("Security.UnsecuredAccessAllowed", ConfigurationEntryScope.ServerWideOnly)]
        public UnsecuredAccessAddressRange UnsecuredAccessAllowed { get; set; }

        internal bool? IsUnsecureAccessSetupValid { get; private set; }

        internal string UnsecureAccessWarningMessage { get; private set; }

        public bool IsCertificateConfigured => string.IsNullOrWhiteSpace(CertificatePath) == false 
            || string.IsNullOrWhiteSpace(CertificateExec) == false;

        public bool AuthenticationEnabled => IsCertificateConfigured;

        [Description("When using SSL Proxy, what is the certificate that the proxy will use to talk to RavenDB")]
        [DefaultValue(null)]
        [ConfigurationEntry("Security.SslProxy.Certificate.Path", ConfigurationEntryScope.ServerWideOnly)]
        public string SslProxyCertificatePath { get; set; }

        [Description("The password for the SSL Proxy certificate")]
        [DefaultValue(null)]
        [ConfigurationEntry("Security.SslProxy.Certificate.Password", ConfigurationEntryScope.ServerWideOnly)]
        public string SslProxyCertificatePassword { get; set; }

        internal static void Validate(RavenConfiguration configuration)
        {
            var serverUrl = configuration.Core.ServerUrl.ToLowerInvariant();
            if (Uri.TryCreate(serverUrl, UriKind.Absolute, out var uri) == false)
                throw new UriFormatException("Unable to parse URL - " + serverUrl);

            var isServerUrlHttps = uri.Scheme.Equals("https", StringComparison.OrdinalIgnoreCase);

            var serverAddresses = DetermineServerIp(uri);
            var unsecuredAccessAddressRange = configuration.Security.UnsecuredAccessAllowed;
            var serverIsWithinUnsecuredAccessRange = serverAddresses.Any(x => SecurityUtils.IsUnsecuredAccessAllowedForAddress(unsecuredAccessAddressRange, x));

            if (configuration.Security.AuthenticationEnabled)
            {
                if (isServerUrlHttps == false)
                    throw new InvalidOperationException(
                        $"When the server certificate in either `{RavenConfiguration.GetKey(x => x.Security.CertificatePath)}` or `{RavenConfiguration.GetKey(x => x.Security.CertificateExec)}`  is specified, the `{RavenConfiguration.GetKey(x => x.Core.ServerUrl)}` must be using https, but was " +
                        serverUrl);
            }
            else
            {
                if (isServerUrlHttps)
                    throw new InvalidOperationException($"Configured server address { configuration.Core.ServerUrl } requires HTTPS. Please set up certification information under { RavenConfiguration.GetKey(x => x.Security.CertificatePath) } configuration key.");

                if (serverIsWithinUnsecuredAccessRange == false)
                {
                    configuration.Security.UnsecureAccessWarningMessage =
                        $"Configured {RavenConfiguration.GetKey(x => x.Core.ServerUrl)} \"{configuration.Core.ServerUrl}\" is not set within allowed unsecured access address range - { configuration.Security.UnsecuredAccessAllowed }. Use a server url within unsecure access address range ({RavenConfiguration.GetKey(x => x.Security.UnsecuredAccessAllowed)} option) or fill in server certificate information.";
                    configuration.Security.IsUnsecureAccessSetupValid = false;
                }
            }

            if (configuration.Security.IsUnsecureAccessSetupValid.HasValue == false)
                configuration.Security.IsUnsecureAccessSetupValid = true;
        }

        private static IPAddress[] DetermineServerIp(Uri serverUri)
        {
            IPAddress[] addresses;
            if (UrlUtil.IsZeros(serverUri.DnsSafeHost))
            {
                addresses = new[] { IPAddress.Parse(serverUri.DnsSafeHost) };
            }
            else
            {
                var getHostAddressesTask = Dns.GetHostAddressesAsync(serverUri.DnsSafeHost);
                if (getHostAddressesTask.Wait(TimeSpan.FromSeconds(30)) == false)
                    throw new InvalidOperationException($"Could not obtain IP address from DNS {serverUri.DnsSafeHost} for 30 seconds.");

                addresses = getHostAddressesTask.Result;
            }

            if (addresses.Length == 0)
                throw new InvalidOperationException($"Could not determine IP address for {serverUri}.");

            return addresses;
        }
    }

    [Flags]
    public enum UnsecuredAccessAddressRange
    {
        None = 0,
        Local = 1,
        PrivateNetwork = 2,
        PublicNetwork = 4
    }
}
