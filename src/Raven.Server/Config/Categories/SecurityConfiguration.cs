using System;
using System.ComponentModel;
using System.Linq;
using System.Net;
using System.Net.Security;
using Raven.Server.Config.Attributes;
using Raven.Server.Config.Settings;
using Raven.Server.Utils;
using Sparrow;

namespace Raven.Server.Config.Categories
{
    [ConfigurationCategory(ConfigurationCategoryType.Security)]
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
        public TimeSetting AuditLogRetentionTime { get; set; }

        [Description("The maximum size of the audit log after which the old files will be deleted")]
        [DefaultValue(null)]
        [MinValue(256)]
        [SizeUnit(SizeUnit.Megabytes)]
        [ConfigurationEntry("Security.AuditLog.RetentionSizeInMb", ConfigurationEntryScope.ServerWideOnly)]
        public Size? AuditLogRetentionSize { get; set; }

        [Description("Will determine whether to compress the audit log files")]
        [DefaultValue(false)]
        [ConfigurationEntry("Security.AuditLog.Compress", ConfigurationEntryScope.ServerWideOnly)]
        public bool AuditLogCompress { get; set; }

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
        [ConfigurationEntry("Security.Certificate.Password", ConfigurationEntryScope.ServerWideOnly, isSecured: true)]
        public string CertificatePassword { get; set; }

        [Description("Deprecated. Use Security.Certificate.Load.Exec along with Security.Certificate.Renew.Exec and Security.Certificate.Change.Exec")]
        [DefaultValue(null)]
        [ConfigurationEntry("Security.Certificate.Exec", ConfigurationEntryScope.ServerWideOnly)]
        public string CertificateExec { get; set; }

        [Description("A command or executable providing a .pfx cluster certificate when invoked by RavenDB. If specified, RavenDB will use HTTPS/SSL for all network activities. The certificate path setting takes precedence over executable configuration option.")]
        [DefaultValue(null)]
        [ConfigurationEntry("Security.Certificate.Load.Exec", ConfigurationEntryScope.ServerWideOnly)]
        public string CertificateLoadExec { get; set; }

        [Description("A command or executable to handle automatic renewals, providing a renewed .pfx cluster certificate. The leader node will invoke the executable once every hour and if a new certificate is received, it will be sent to the other nodes. The executable specified in Security.Certificate.Change.Exec will then be used to persist the certificate across the cluster.")]
        [DefaultValue(null)]
        [ConfigurationEntry("Security.Certificate.Renew.Exec", ConfigurationEntryScope.ServerWideOnly)]
        public string CertificateRenewExec { get; set; }

        [Description("A command or executable handling a change in the cluster certificate. When invoked, RavenDB will send the new cluster certificate to this executable, giving the follower nodes a way to persist the new certificate.")]
        [DefaultValue(null)]
        [ConfigurationEntry("Security.Certificate.Change.Exec", ConfigurationEntryScope.ServerWideOnly)]
        public string CertificateChangeExec { get; set; }

        [Description("Deprecated. Use Security.Certificate.Load.Exec and Security.Certificate.Load.Exec.Arguments")]
        [DefaultValue(null)]
        [ConfigurationEntry("Security.Certificate.Exec.Arguments", ConfigurationEntryScope.ServerWideOnly, isSecured: true)]
        public string CertificateExecArguments { get; set; }

        [Description("The command line arguments for the 'Security.Certificate.Load.Exec' command or executable.")]
        [DefaultValue(null)]
        [ConfigurationEntry("Security.Certificate.Load.Exec.Arguments", ConfigurationEntryScope.ServerWideOnly, isSecured: true)]
        public string CertificateLoadExecArguments { get; set; }

        [Description("The command line arguments for the 'Security.Certificate.Renew.Exec' command or executable.")]
        [DefaultValue(null)]
        [ConfigurationEntry("Security.Certificate.Renew.Exec.Arguments", ConfigurationEntryScope.ServerWideOnly, isSecured: true)]
        public string CertificateRenewExecArguments { get; set; }

        [Description("The command line arguments for the 'Security.Certificate.Change.Exec' command or executable.")]
        [DefaultValue(null)]
        [ConfigurationEntry("Security.Certificate.Change.Exec.Arguments", ConfigurationEntryScope.ServerWideOnly, isSecured: true)]
        public string CertificateChangeExecArguments { get; set; }

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
        [ConfigurationEntry("Security.MasterKey.Exec.Arguments", ConfigurationEntryScope.ServerWideOnly, isSecured: true)]
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

        [Description("EXPERT: A command or executable to validate a server authentication request. " +
                     "RavenDB will execute: command [user-arg-1] ... [user-arg-n] <sender-url> <base64-certificate> <errors>. " +
                     "The executable will return a case-insensitive boolean string through the standard output (e.g. true, false) indicating whether to approve the connection.")]
        [DefaultValue(null)]
        [ConfigurationEntry("Security.Certificate.Validation.Exec", ConfigurationEntryScope.ServerWideOnly)]
        public string CertificateValidationExec { get; set; }

        [Description("EXPERT: The optional user arguments for the 'Security.Certificate.Validation.Exec' command or executable. The arguments must be escaped for the command line.")]
        [DefaultValue(null)]
        [ConfigurationEntry("Security.Certificate.Validation.Exec.Arguments", ConfigurationEntryScope.ServerWideOnly, isSecured: true)]
        public string CertificateValidationExecArguments { get; set; }

        [Description("The number of seconds to wait for the 'Security.Certificate.Validation.Exec' executable to exit. Default: 5 seconds")]
        [DefaultValue(5)]
        [TimeUnit(TimeUnit.Seconds)]
        [ConfigurationEntry("Security.Certificate.Validation.Exec.TimeoutInSec", ConfigurationEntryScope.ServerWideOnly)]
        public TimeSetting CertificateValidationExecTimeout { get; set; }

        [Description("EXPERT: Defines a list of supported TLS Cipher Suites. Values must be semicolon separated. Default: null (Operating System defaults)")]
        [DefaultValue(null)]
        [ConfigurationEntry("Security.TlsCipherSuites", ConfigurationEntryScope.ServerWideOnly)]
        public TlsCipherSuite[] TlsCipherSuites { get; set; }

        internal bool? IsUnsecureAccessSetupValid { get; private set; }

        internal string UnsecureAccessWarningMessage { get; private set; }

        public bool IsCertificateConfigured =>
            string.IsNullOrWhiteSpace(CertificatePath) == false ||
            string.IsNullOrWhiteSpace(CertificateLoadExec) == false;

        public bool AuthenticationEnabled => IsCertificateConfigured;

#if !RVN
        internal static void Validate(RavenConfiguration configuration)
        {
            foreach (var sUrl in configuration.Core.ServerUrls)
            {
                var serverUrl = sUrl.ToLowerInvariant();

                if (Uri.TryCreate(serverUrl, UriKind.Absolute, out var uri) == false)
                    throw new UriFormatException("Unable to parse URL - " + serverUrl);

                var isServerUrlHttps = uri.Scheme.Equals("https", StringComparison.OrdinalIgnoreCase);

                if (configuration.Security.AuthenticationEnabled)
                {
                    if (isServerUrlHttps == false)
                        throw new InvalidOperationException(
                            $"When the server certificate in either `{RavenConfiguration.GetKey(x => x.Security.CertificatePath)}` or `{RavenConfiguration.GetKey(x => x.Security.CertificateLoadExec)}` is specified, the `{RavenConfiguration.GetKey(x => x.Core.ServerUrls)}` must be using HTTPS, but was " +
                            serverUrl);
                }
                else
                {
                    if (isServerUrlHttps)
                        throw new InvalidOperationException($"Configured server address { string.Join(", ", configuration.Core.ServerUrls) } requires HTTPS. Please set up certification information under { RavenConfiguration.GetKey(x => x.Security.CertificatePath) } configuration key.");

                    var serverAddresses = DetermineServerIp(uri);
                    var unsecuredAccessAddressRange = configuration.Security.UnsecuredAccessAllowed;

                    var serverIsWithinUnsecuredAccessRange = serverAddresses.Any(x => SecurityUtils.IsUnsecuredAccessAllowedForAddress(unsecuredAccessAddressRange, x));

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

                try
                {
                    addresses = getHostAddressesTask.Result;
                }
                catch (Exception e)
                {
                    throw new InvalidOperationException($"Failed to get IP address of {serverUri.DnsSafeHost} host", e);
                }
            }

            if (addresses.Length == 0)
                throw new InvalidOperationException($"Could not determine IP address for {serverUri}.");

            return addresses;
        }
#endif
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
