using System.ComponentModel;
using Raven.Server.Config.Attributes;
using Raven.Server.Config.Settings;
using Raven.Server.Utils;
using System;
using System.Linq;
using Microsoft.Extensions.Configuration;
using Raven.Server.ServerWide;

namespace Raven.Server.Config.Categories
{
    public class CoreConfiguration : ConfigurationCategory
    {
        [Description("The URLs which the server should listen to. By default we listen to localhost:8080")]
        [DefaultValue("http://localhost:8080")]
        [ConfigurationEntry("ServerUrl", ConfigurationEntryScope.ServerWideOnly)]
        public string ServerUrl { get; set; }

        [Description("If not specified, will use the server url host and random port. If it just a number specify, will use that port. Otherwise, will bind to the host & port specified")]
        [DefaultValue(null)]
        [ConfigurationEntry("ServerUrl.Tcp", ConfigurationEntryScope.ServerWideOnly)]
        public string TcpServerUrl { get; set; }

        [Description("The URL under which server is publicly available, used for inter-node communication and access from behind a firewall, proxy etc.")]
        [DefaultValue(null)]
        [ConfigurationEntry("PublicServerUrl", ConfigurationEntryScope.ServerWideOnly)]
        public UriSetting? PublicServerUrl { get; set; }

        [Description("Public TCP address")]
        [DefaultValue(null)]
        [ConfigurationEntry("PublicServerUrl.Tcp", ConfigurationEntryScope.ServerWideOnly)]
        public UriSetting? PublicTcpServerUrl { get; set; }

        [Description("Whether the database should run purely in memory. When running in memory, nothing is written to disk and if the server is restarted all data will be lost. This is mostly useful for testing.")]
        [DefaultValue(false)]
        [ConfigurationEntry("RunInMemory", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public bool RunInMemory { get; set; }

        [Description("The directory for the RavenDB resource. You can use the ~/ prefix to refer to RavenDB's base directory.")]
        [DefaultValue(@"~/Databases/{name}")]
        [ConfigurationEntry("DataDir", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public PathSetting DataDirectory { get; set; }

        [Description("Indicates if we should throw an exception if any index could not be opened")]
        [DefaultValue(false)]
        [ConfigurationEntry("ThrowIfAnyIndexCannotBeOpened", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public bool ThrowIfAnyIndexCannotBeOpened { get; set; }

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
                GetNodeHost(serverWebUri, ServerUrl),
                serverWebUri.Port);

            return UrlUtil.TrimTrailingSlash(httpUriBuilder.Uri.ToString());
        }

        internal string GetNodeTcpServerUrl(string serverWebUrl, int actualPort)
        {
            if (PublicTcpServerUrl.HasValue)
                return UrlUtil.TrimTrailingSlash(PublicTcpServerUrl.Value.UriValue);

            if (Uri.TryCreate(serverWebUrl, UriKind.Absolute, out var serverWebUri) == false)
                throw new InvalidOperationException($"Could not parse server web url: {serverWebUrl}");

            var tcpUriBuilder = new UriBuilder("tcp", GetNodeHost(serverWebUri, ServerUrl), actualPort);

            return UrlUtil.TrimTrailingSlash(tcpUriBuilder.Uri.ToString());
        }

        private string GetNodeHost(Uri serverWebUri, string serverUrlSettingValue)
        {
            if (Uri.TryCreate(serverUrlSettingValue, UriKind.Absolute, out var serverUrlSettingUri)
                && UrlUtil.IsZeros(serverUrlSettingUri.Host))
            {
                return Environment.MachineName;
            }

            return serverWebUri.Host;
        }

        internal void ValidateServerUrls()
        {
            if (ServerUrl != null)
                ValidateServerUrl(ServerUrl, new[] { "http", "https" }, RavenConfiguration.GetKey(x => x.Core.ServerUrl));

            if (TcpServerUrl != null
                && ushort.TryParse(TcpServerUrl, out var _) == false)
                ValidateServerUrl(TcpServerUrl, new[] { "tcp" }, RavenConfiguration.GetKey(x => x.Core.TcpServerUrl));

        }

        internal void ValidatePublicUrls()
        {
            if (PublicServerUrl.HasValue)
                ValidatePublicUrl(PublicServerUrl.Value.UriValue, RavenConfiguration.GetKey(x => x.Core.PublicServerUrl));

            if (PublicTcpServerUrl.HasValue)
                ValidatePublicUrl(PublicTcpServerUrl.Value.UriValue, RavenConfiguration.GetKey(x => x.Core.PublicTcpServerUrl));
        }

        private void ValidateServerUrl(string url, string[] expectedSchemes, string confKey)
        {
            if (Uri.TryCreate(url, UriKind.Absolute, out var parsedUri) == false)
                throw new ArgumentException($"'{url}' is an invalid URI.");

            if (expectedSchemes.Any(x => x == parsedUri.Scheme) == false)
                throw new ArgumentException($"URI scheme '{ parsedUri.Scheme }' is invalid for '{confKey}' configuration setting, it must be one of the following: { string.Join(", ", expectedSchemes) }.");
        }

        private void ValidateSchemePublicVsBoundUrl()
        {
            if (PublicServerUrl.HasValue == false)
                return;

            if (Uri.TryCreate(ServerUrl, UriKind.Absolute, out var serverUri) == false)
                throw new ArgumentException($"ServerUrl could not be parsed: {ServerUrl}.");

            if (Uri.TryCreate(PublicServerUrl.Value.UriValue, UriKind.Absolute, out var publicServerUri) == false)
                throw new ArgumentException($"PublicServerUrl could not be parsed: {PublicServerUrl}.");

            if (serverUri.Scheme != publicServerUri.Scheme)
                throw new ArgumentException($"ServerUrl and PublicServerUrl schemes do not match: {ServerUrl} and {PublicServerUrl.Value.UriValue}.");
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
}
