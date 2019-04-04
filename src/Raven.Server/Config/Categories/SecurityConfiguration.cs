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
        [Description("Disable automatic redirection when listening to HTTPS. By default, when using port 443, RavenDB redirects all incoming HTTP traffic on port 80 to HTTPS on port 443.")]
        [DefaultValue(false)]
        [ConfigurationEntry("Security.DisableHttpsRedirection", ConfigurationEntryScope.ServerWideOnly)]
        public bool DisableHttpsRedirection { get; set; }

        [Description("The path to a folder where RavenDB will store the access audit logs")]
        [DefaultValue(null)]
        [ConfigurationEntry("Security.AuditLog.FolderPath", ConfigurationEntryScope.ServerWideOnly)]
        public PathSetting AuditLogPath { get; set; }

        [Description("How far back we should retain audit log entries")]
        [DefaultValue(365 * 24)]
        [TimeUnit(TimeUnit.Hours)]
        [ConfigurationEntry("Security.AuditLog.RetentionTimeInHrs", ConfigurationEntryScope.ServerWideOnly)]
        [ConfigurationEntry("Security.AuditLog.RetentionTimeInHours", ConfigurationEntryScope.ServerWideOnly)]
        public TimeSetting AuditLogRetention { get; set; }

        [Description("The path to .pfx certificate file. If specified, RavenDB will use HTTPS/SSL for all network activities. Certificate setting priority order: 1) Path 2) Executable")]
        [DefaultValue(null)]
        [ConfigurationEntry("Security.Certificate.Path", ConfigurationEntryScope.ServerWideOnly)]
        public string CertificatePath { get; set; }

        [Description("EXPERT: Whether RavenDB will consider memory lock error to be catastrophic. This is used with encrypted databases to ensure that temporary buffers are never written to disk and are locked to memory. Setting this to true is not recommended and should be done only after proper security analysis has been performed.")]
        [DefaultValue(false)]
        [ConfigurationEntry("Security.DoNotConsiderMemoryLockFailureAsCatastrophicError", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public bool DoNotConsiderMemoryLockFailureAsCatastrophicError { get; set; }

        [Description("The (optional) password of the .pfx certificate file.")]
        [DefaultValue(null)]
        [ConfigurationEntry("Security.Certificate.Password", ConfigurationEntryScope.ServerWideOnly)]
        public string CertificatePassword { get; set; }

        [Description("A command or executable providing a .pfx certificate file. If specified, RavenDB will use HTTPS/SSL for all network activities. The certificate path setting takes precedence over executable configuration option.")]
        [DefaultValue(null)]
        [ConfigurationEntry("Security.Certificate.Exec", ConfigurationEntryScope.ServerWideOnly)]
        public string CertificateExec { get; set; }

        [Description("A command or executable providing a .pfx certificate file. If specified, RavenDB will use HTTPS/SSL for all network activities. The certificate path setting takes precedence over executable configuration option.")]
        [DefaultValue(null)]
        [ConfigurationEntry("Security.Certificate.Exec.Load", ConfigurationEntryScope.ServerWideOnly)]
        public string CertificateExecLoad { get; set; }

        [Description("A command or executable providing a .pfx certificate file. If specified, RavenDB will use HTTPS/SSL for all network activities. The certificate path setting takes precedence over executable configuration option.")]
        [DefaultValue(null)]
        [ConfigurationEntry("Security.Certificate.Exec.Renew", ConfigurationEntryScope.ServerWideOnly)]
        public string CertificateExecRenew { get; set; }

        [Description("A command or executable providing a .pfx certificate file. If specified, RavenDB will use HTTPS/SSL for all network activities. The certificate path setting takes precedence over executable configuration option.")]
        [DefaultValue(null)]
        [ConfigurationEntry("Security.Certificate.Exec.OnCertificateChange", ConfigurationEntryScope.ServerWideOnly)]
        public string CertificateExecOnCertificateChange { get; set; }

        [Description("The command line arguments for the 'Security.Certificate.Exec' command or executable.")]
        [DefaultValue(null)]
        [ConfigurationEntry("Security.Certificate.Exec.Arguments", ConfigurationEntryScope.ServerWideOnly)]
        public string CertificateExecArguments { get; set; }

        [Description("The command line arguments for the 'Security.Certificate.Exec.Load' command or executable.")]
        [DefaultValue(null)]
        [ConfigurationEntry("Security.Certificate.Exec.Load.Arguments", ConfigurationEntryScope.ServerWideOnly)]
        public string CertificateExecLoadArguments { get; set; }

        [Description("The command line arguments for the 'Security.Certificate.Exec.Renew' command or executable.")]
        [DefaultValue(null)]
        [ConfigurationEntry("Security.Certificate.Exec.Renew.Arguments", ConfigurationEntryScope.ServerWideOnly)]
        public string CertificateExecRenewArguments { get; set; }

        [Description("The command line arguments for the 'Security.Certificate.Exec.OnCertificateChange' command or executable.")]
        [DefaultValue(null)]
        [ConfigurationEntry("Security.Certificate.Exec.OnCertificateChange.Arguments", ConfigurationEntryScope.ServerWideOnly)]
        public string CertificateExecOnCertificateChangeArguments { get; set; }
        
        [Description("The number of seconds to wait for the certificate executable to exit. Default: 30 seconds")]
        [DefaultValue(30)]
        [TimeUnit(TimeUnit.Seconds)]
        [ConfigurationEntry("Security.Certificate.Exec.TimeoutInSec", ConfigurationEntryScope.ServerWideOnly)]
        public TimeSetting CertificateExecTimeout { get; set; }

        [Description("The E-mail address associated with the Let's Encrypt certificate. Used for renewal requests.")]
        [DefaultValue(null)]
        [ConfigurationEntry("Security.Certificate.LetsEncrypt.Email", ConfigurationEntryScope.ServerWideOnly)]
        public string CertificateLetsEncryptEmail { get; set; }

        [Description("The path of the (256-bit) Master Key. If specified, RavenDB will use this key to protect secrets.")]
        [DefaultValue(null)]
        [ConfigurationEntry("Security.MasterKey.Path", ConfigurationEntryScope.ServerWideOnly)]
        public string MasterKeyPath { get; set; }

        [Description("A command or executable to run which will provide a (256-bit) Master Key, If specified, RavenDB will use this key to protect secrets.")]
        [DefaultValue(null)]
        [ConfigurationEntry("Security.MasterKey.Exec", ConfigurationEntryScope.ServerWideOnly)]
        public string MasterKeyExec { get; set; }

        [Description("The command line arguments for the 'Security.MasterKey.Exec' command or executable.")]
        [DefaultValue(null)]
        [ConfigurationEntry("Security.MasterKey.Exec.Arguments", ConfigurationEntryScope.ServerWideOnly)]
        public string MasterKeyExecArguments { get; set; }

        [Description("The number of seconds to wait for the Master Key executable to exit. Default: 30 seconds")]
        [DefaultValue(30)]
        [TimeUnit(TimeUnit.Seconds)]
        [ConfigurationEntry("Security.MasterKey.Exec.TimeoutInSec", ConfigurationEntryScope.ServerWideOnly)]
        public TimeSetting MasterKeyExecTimeout { get; set; }

        [Description("If authentication is disabled, set address range type for which server access is unsecured (None | Local | PrivateNetwork | PublicNetwork).")]
        [DefaultValue(UnsecuredAccessAddressRange.Local)]
        [ConfigurationEntry("Security.UnsecuredAccessAllowed", ConfigurationEntryScope.ServerWideOnly)]
        public UnsecuredAccessAddressRange UnsecuredAccessAllowed { get; set; }

        [Description("Well known certificate thumbprints that will be trusted by the server as cluster admins.")]
        [DefaultValue(null)]
        [ConfigurationEntry("Security.WellKnownCertificates.Admin", ConfigurationEntryScope.ServerWideOnly)]
        public string[] WellKnownAdminCertificates { get; set; }

        [Description("Well known issuer 'Public Key Pinning Hashes' that will be used to validate a new client certificate when the issuer's certificate has changed.")]
        [DefaultValue(null)]
        [ConfigurationEntry("Security.WellKnownIssuerHashes.Admin", ConfigurationEntryScope.ServerWideOnly)]
        public string[] WellKnownIssuerHashes { get; set; }

        internal bool? IsUnsecureAccessSetupValid { get; private set; }

        internal string UnsecureAccessWarningMessage { get; private set; }

        public bool IsCertificateConfigured =>
            string.IsNullOrWhiteSpace(CertificatePath) == false ||
            string.IsNullOrWhiteSpace(CertificateExecLoad) == false;

        public bool AuthenticationEnabled => IsCertificateConfigured;

        internal static void Validate(RavenConfiguration configuration)
        {
            foreach (var sUrl in configuration.Core.ServerUrls)
            {
                var serverUrl = sUrl.ToLowerInvariant();

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
                            $"When the server certificate in either `{RavenConfiguration.GetKey(x => x.Security.CertificatePath)}` or `{RavenConfiguration.GetKey(x => x.Security.CertificateExecLoad)}` is specified, the `{RavenConfiguration.GetKey(x => x.Core.ServerUrls)}` must be using https, but was " +
                            serverUrl);
                }
                else
                {
                    if (isServerUrlHttps)
                        throw new InvalidOperationException($"Configured server address { string.Join(", ", configuration.Core.ServerUrls) } requires HTTPS. Please set up certification information under { RavenConfiguration.GetKey(x => x.Security.CertificatePath) } configuration key.");

                    if (serverIsWithinUnsecuredAccessRange == false)
                    {
                        configuration.Security.UnsecureAccessWarningMessage =
                            $"Configured {RavenConfiguration.GetKey(x => x.Core.ServerUrls)} \"{string.Join(", ", configuration.Core.ServerUrls)}\" is not set within allowed unsecured access address range - { configuration.Security.UnsecuredAccessAllowed }. Use a server url within unsecure access address range ({RavenConfiguration.GetKey(x => x.Security.UnsecuredAccessAllowed)} option) or fill in server certificate information.";
                        configuration.Security.IsUnsecureAccessSetupValid = false;
                    }
                }

                if (configuration.Security.IsUnsecureAccessSetupValid.HasValue == false)
                    configuration.Security.IsUnsecureAccessSetupValid = true;
            }
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
