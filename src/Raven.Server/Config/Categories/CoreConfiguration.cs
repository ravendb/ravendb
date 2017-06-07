using System.ComponentModel;
using Raven.Server.Config.Attributes;
using Raven.Server.Config.Settings;
using Raven.Server.Utils;
using System;
using Microsoft.Extensions.Configuration;
using Raven.Server.ServerWide;

namespace Raven.Server.Config.Categories
{
    public class CoreConfiguration : ConfigurationCategory
    {
        [Description("The directory into which RavenDB will search the studio files, defaults to the base directory")]
        [DefaultValue(null)]
        [ConfigurationEntry("Raven/StudioDirectory")]
        public string StudioDirectory { get; set; }

        [Description("The directory into which RavenDB will write the logs, for relative path, the applciation base directory is used")]
        [DefaultValue("Logs")]
        [ConfigurationEntry("Raven/LogsDirectory")]
        public string LogsDirectory { get; set; }

        [Description("The logs level which RavenDB will use (None, Information, Operations)")]
        [DefaultValue("Operations")]
        [ConfigurationEntry("Raven/LogsLevel")]
        public string LogLevel { get; set; }

        [Description("The URLs which the server should listen to. By default we listen to localhost:8080")]
        [DefaultValue("http://localhost:8080")]
        [ConfigurationEntry("Raven/ServerUrl")]
        public string ServerUrl { get; set; }

        [Description("If not specified, will use the server url host and random port. If it just a number specify, will use that port. Otherwise, will bind to the host & port specified")]
        [DefaultValue(null)]
        [ConfigurationEntry("Raven/ServerUrl/Tcp")]
        public string TcpServerUrl { get; set; }


        [Description("The URL under which server is publicly available, used for inter-node communication and access from behind a firewall, proxy etc.")]
        [DefaultValue(null)]
        [ConfigurationEntry("Raven/PublicServerUrl", isServerWideOnly: true)]
        public UriSetting? PublicServerUrl { get; set; }

        [Description("Public TCP address")]
        [DefaultValue(null)]
        [ConfigurationEntry("Raven/PublicServerUrl/Tcp", isServerWideOnly: true)]
        public UriSetting? PublicTcpServerUrl { get; set; }

        [Description("Whether the database should run purely in memory. When running in memory, nothing is written to disk and if the server is restarted all data will be lost. This is mostly useful for testing.")]
        [DefaultValue(false)]
        [ConfigurationEntry("Raven/RunInMemory")]
        public bool RunInMemory { get; set; }

        [Description("The directory for the RavenDB resource. You can use the ~/ prefix to refer to RavenDB's base directory.")]
        [DefaultValue(@"~/{pluralizedResourceType}/{name}")]
        [ConfigurationEntry("Raven/DataDir")]
        public PathSetting DataDirectory { get; set; }

        [Description("The time to wait before canceling a database operation such as load (many) or query")]
        [DefaultValue(5)]
        [TimeUnit(TimeUnit.Minutes)]
        [ConfigurationEntry("Raven/DatabaseOperationTimeoutInMin")]
        [LegacyConfigurationEntry("Raven/DatabaseOperationTimeout")]
        public TimeSetting DatabaseOperationTimeout { get; set; }

        [Description("Indicates if we should throw an exception if any index could not be opened")]
        [DefaultValue(false)]
        [ConfigurationEntry("Raven/ThrowIfAnyIndexOrTransformerCouldNotBeOpened")]
        public bool ThrowIfAnyIndexOrTransformerCouldNotBeOpened { get; set; }

        public override void Initialize(IConfigurationRoot settings, IConfigurationRoot serverWideSettings, ResourceType type, string resourceName)
        {
            base.Initialize(settings, serverWideSettings, type, resourceName);
            ValidatePublicUrls();
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

        internal void ValidatePublicUrls()
        {
            if (PublicServerUrl.HasValue)
                ValidatePublicUrl(PublicServerUrl.Value.UriValue, RavenConfiguration.GetKey(x => x.Core.PublicServerUrl));

            if (PublicTcpServerUrl.HasValue)
                ValidatePublicUrl(PublicTcpServerUrl.Value.UriValue, RavenConfiguration.GetKey(x => x.Core.PublicTcpServerUrl));
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