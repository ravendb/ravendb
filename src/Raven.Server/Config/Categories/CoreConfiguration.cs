using System.ComponentModel;
using Raven.Server.Config.Attributes;
using Raven.Server.Config.Settings;
using Raven.Server.Utils;
using System;
using System.Linq;
using Microsoft.Extensions.Configuration;
using Raven.Server.Commercial;
using Raven.Server.ServerWide;

namespace Raven.Server.Config.Categories
{
    public class CoreConfiguration : ConfigurationCategory
    {
        [Description("The URLs which the server should listen to. By default we listen to localhost:8080")]
        [DefaultValue("http://localhost:8080")]
        [ConfigurationEntry("ServerUrl", ConfigurationEntryScope.ServerWideOnly)]
        public string[] ServerUrls { get; set; }

        [Description("If not specified, will use the server url host and random port. If it just a number specify, will use that port. Otherwise, will bind to the host & port specified")]
        [DefaultValue(null)]
        [ConfigurationEntry("ServerUrl.Tcp", ConfigurationEntryScope.ServerWideOnly)]
        public string[] TcpServerUrls { get; set; }

        [Description("The URL under which server is publicly available, used for inter-node communication and access from behind a firewall, proxy etc.")]
        [DefaultValue(null)]
        [ConfigurationEntry("PublicServerUrl", ConfigurationEntryScope.ServerWideOnly)]
        public UriSetting? PublicServerUrl { get; set; }

        [Description("Public TCP address")]
        [DefaultValue(null)]
        [ConfigurationEntry("PublicServerUrl.Tcp", ConfigurationEntryScope.ServerWideOnly)]
        public UriSetting? PublicTcpServerUrl { get; set; }

        [Description("External IP address")]
        [DefaultValue(null)]
        [ConfigurationEntry("ExternalIp", ConfigurationEntryScope.ServerWideOnly)]
        public string ExternalIp { get; set; }

        [Description("Whether the database should run purely in memory. When running in memory, nothing is written to disk and if the server is restarted all data will be lost. This is mostly useful for testing.")]
        [DefaultValue(false)]
        [ConfigurationEntry("RunInMemory", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public bool RunInMemory { get; set; }

        [Description("The directory for the RavenDB resource. Relative path will be located under the application base directory.")]
        [DefaultValue(@"Databases/{name}")]
        [ConfigurationEntry("DataDir", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public PathSetting DataDirectory { get; set; }

        [Description("Databases can only be created under the DataDir path.")]
        [DefaultValue(false)]
        [ConfigurationEntry("DataDir.EnforcePath", ConfigurationEntryScope.ServerWideOnly)]
        public bool EnforceDataDirectoryPath { get; set; }

        [Description("Determines what kind of security was chosen during setup.")]
        [DefaultValue(SetupMode.None)]
        [ConfigurationEntry("Setup.Mode", ConfigurationEntryScope.ServerWideOnly)]
        public SetupMode SetupMode { get; set; }

        [Description("The URLs which the server should contact when requesting certificates from using the ACME protocol.")]
        [DefaultValue("https://acme-v02.api.letsencrypt.org/directory")]
        [ConfigurationEntry("AcmeUrl", ConfigurationEntryScope.ServerWideOnly)]
        public string AcmeUrl { get; set; }

        [Description("Indicates if we should throw an exception if any index could not be opened")]
        [DefaultValue(false)]
        [ConfigurationEntry("ThrowIfAnyIndexCannotBeOpened", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public bool ThrowIfAnyIndexCannotBeOpened { get; set; }

        [Description("Indicates what set of features should be available")]
        [DefaultValue(FeaturesAvailability.Stable)]
        [ConfigurationEntry("Features.Availability", ConfigurationEntryScope.ServerWideOnly)]
        public FeaturesAvailability FeaturesAvailability { get; set; }

        public override void Initialize(IConfigurationRoot settings, IConfigurationRoot serverWideSettings, ResourceType type, string resourceName)
        {
            base.Initialize(settings, serverWideSettings, type, resourceName);

            if (type != ResourceType.Server)
                return;

            ValidateServerUrls();
            ValidatePublicUrls();
            ValidateSchemePublicVsBoundUrl();
        }

        internal string GetNodeHttpServerUrl(string serverWebUrl)
        {
            if (PublicServerUrl.HasValue)
                return UrlUtil.TrimTrailingSlash(PublicServerUrl.Value.UriValue);

            if (Uri.TryCreate(serverWebUrl, UriKind.Absolute, out var serverWebUri) == false)
                throw new InvalidOperationException($"Could not parse server web url: {serverWebUrl}");

            var httpUriBuilder = new UriBuilder(
                serverWebUri.Scheme,
                GetNodeHost(serverWebUri, ServerUrls[0]),
                serverWebUri.Port);

            return UrlUtil.TrimTrailingSlash(httpUriBuilder.Uri.ToString());
        }

        internal string GetNodeTcpServerUrl(string serverWebUrl, int actualPort)
        {
            if (PublicTcpServerUrl.HasValue)
                return UrlUtil.TrimTrailingSlash(PublicTcpServerUrl.Value.UriValue);

            if (PublicServerUrl.HasValue)
            {
                if (Uri.TryCreate(PublicServerUrl.Value.UriValue, UriKind.Absolute, out var publicServerUri) == false)
                    throw new ArgumentException($"PublicServerUrl could not be parsed: {PublicServerUrl}.");

                var tcpUriBuilder = new UriBuilder("tcp", publicServerUri.Host, actualPort);

                return UrlUtil.TrimTrailingSlash(tcpUriBuilder.Uri.ToString());
            }
            else
            {
                if (Uri.TryCreate(serverWebUrl, UriKind.Absolute, out var serverWebUri) == false)
                    throw new InvalidOperationException($"Could not parse server web url: {serverWebUrl}");

                var tcpUriBuilder = new UriBuilder("tcp", GetNodeHost(serverWebUri, ServerUrls[0]), actualPort);

                return UrlUtil.TrimTrailingSlash(tcpUriBuilder.Uri.ToString());
            }
        }

        private string GetNodeHost(Uri serverWebUri, string serverUrlSettingValue)
        {
            if (serverWebUri != null
                && UrlUtil.IsZeros(serverWebUri.Host) == false)
                return serverWebUri.Host;

            if (Uri.TryCreate(serverUrlSettingValue, UriKind.Absolute, out var serverUrlSettingUri))
            {
                if (UrlUtil.IsZeros(serverUrlSettingUri.Host))
                    return Environment.MachineName;

                return serverUrlSettingUri.Host;
            }

            throw new InvalidOperationException($"Arguments '{nameof(serverWebUri)}' and '{nameof(serverUrlSettingValue)}' are invalid.");
        }

        internal void ValidateServerUrls()
        {
            if (ServerUrls != null)
                ValidateServerUrl(ServerUrls, new[] { "http", "https" }, RavenConfiguration.GetKey(x => x.Core.ServerUrls));

            if (TcpServerUrls != null)
            {
                if (TcpServerUrls.Length == 1 && ushort.TryParse(TcpServerUrls[0], out var _))
                    return;

                ValidateServerUrl(TcpServerUrls, new[] { "tcp" }, RavenConfiguration.GetKey(x => x.Core.TcpServerUrls));
            }
        }

        internal void ValidatePublicUrls()
        {
            if (PublicServerUrl.HasValue)
                ValidatePublicUrl(PublicServerUrl.Value.UriValue, RavenConfiguration.GetKey(x => x.Core.PublicServerUrl));
            else if (ServerUrls.Length > 1)
                throw new ArgumentException($"Configuration key '{RavenConfiguration.GetKey(x => x.Core.PublicServerUrl)}' must be specified when there is more than one '{RavenConfiguration.GetKey(x => x.Core.ServerUrls)}'.");

            if (PublicTcpServerUrl.HasValue)
                ValidatePublicUrl(PublicTcpServerUrl.Value.UriValue, RavenConfiguration.GetKey(x => x.Core.PublicTcpServerUrl));
            else if (TcpServerUrls != null && TcpServerUrls.Length > 1)
                throw new ArgumentException($"Configuration key '{RavenConfiguration.GetKey(x => x.Core.PublicTcpServerUrl)}' must be specified when there is more than one '{RavenConfiguration.GetKey(x => x.Core.TcpServerUrls)}'.");
        }

        private static void ValidateServerUrl(string[] urls, string[] expectedSchemes, string configurationKey)
        {
            Uri firstUri = null;
            foreach (var url in urls)
            {
                if (Uri.TryCreate(url, UriKind.Absolute, out var parsedUri) == false)
                    throw new ArgumentException($"'{url}' is an invalid URI.");

                if (expectedSchemes.Any(x => x == parsedUri.Scheme) == false)
                    throw new ArgumentException($"URI scheme '{ parsedUri.Scheme }' is invalid for '{configurationKey}' configuration setting, it must be one of the following: { string.Join(", ", expectedSchemes) }.");

                if (firstUri == null)
                {
                    firstUri = parsedUri;
                    continue;
                }

                if (string.Equals(firstUri.Scheme, parsedUri.Scheme, StringComparison.OrdinalIgnoreCase) == false)
                    throw new ArgumentException($"URI '{url}' scheme does not match. Expected '{firstUri.Scheme}'. Was '{parsedUri.Scheme}'.");

                if (firstUri.Port != parsedUri.Port)
                    throw new ArgumentException($"URI '{url}' port does not match. Expected '{firstUri.Port}'. Was '{parsedUri.Port}'.");
            }
        }

        private void ValidateSchemePublicVsBoundUrl()
        {
            if (PublicServerUrl.HasValue == false)
                return;

            if (Uri.TryCreate(ServerUrls[0], UriKind.Absolute, out var serverUri) == false)
                throw new ArgumentException($"ServerUrl could not be parsed: {string.Join(", ", ServerUrls)}.");

            if (Uri.TryCreate(PublicServerUrl.Value.UriValue, UriKind.Absolute, out var publicServerUri) == false)
                throw new ArgumentException($"PublicServerUrl could not be parsed: {PublicServerUrl}.");

            if (serverUri.Scheme != publicServerUri.Scheme)
                throw new ArgumentException($"ServerUrl and PublicServerUrl schemes do not match: {string.Join(", ", ServerUrls)} and {PublicServerUrl.Value.UriValue}.");
        }

        private void ValidatePublicUrl(string uriString, string optName)
        {
            if (Uri.TryCreate(uriString, UriKind.Absolute, out var parsedUri) == false)
                throw new ArgumentException($"Invalid {optName} configuration option: {uriString}.");

            if (parsedUri.Port == 0 || parsedUri.Port == -1)
                throw new ArgumentException($"Invalid port value in {optName} configuration option: {parsedUri.Port}.");

            if (UrlUtil.IsZeros(parsedUri.Host))
                throw new ArgumentException($"Invalid host value in {optName} configuration option: {parsedUri.Host}");
        }
    }

    public enum FeaturesAvailability
    {
        Stable,
        Experimental
    }
}
