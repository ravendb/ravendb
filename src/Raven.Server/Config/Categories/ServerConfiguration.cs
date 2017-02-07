using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.ComponentModel;
using Microsoft.Extensions.Configuration;
using Raven.Server.Config.Attributes;
using Raven.Server.Config.Settings;
using Raven.Server.ServerWide;

namespace Raven.Server.Config.Categories
{
    public class ServerConfiguration : ConfigurationCategory
    {
        [DefaultValue(512)]
        [ConfigurationEntry("Raven/Server/MaxConcurrentRequests")]
        [LegacyConfigurationEntry("Raven/MaxConcurrentServerRequests")]
        public int MaxConcurrentRequests { get; set; }

        [DefaultValue(50)]
        [ConfigurationEntry("Raven/Server/MaxConcurrentRequestsForDatabaseDuringLoad")]
        [LegacyConfigurationEntry("Raven/MaxConcurrentRequestsForDatabaseDuringLoad")]
        public int MaxConcurrentRequestsForDatabaseDuringLoad { get; set; }

        [DefaultValue(30)]
        [TimeUnit(TimeUnit.Seconds)]
        [ConfigurationEntry("Raven/Server/MaxTimeForTaskToWaitForDatabaseToLoadInSec")]
        [LegacyConfigurationEntry("Raven/MaxSecondsForTaskToWaitForDatabaseToLoad")]
        public TimeSetting MaxTimeForTaskToWaitForDatabaseToLoad { get; set; }

        [DefaultValue(192)]
        [ConfigurationEntry("Raven/Server/MaxConcurrentMultiGetRequests")]
        [LegacyConfigurationEntry("Raven/MaxConcurrentMultiGetRequests")]
        public int MaxConcurrentMultiGetRequests { get; set; }


        [Description("Determine the value of the Access-Control-Allow-Origin header sent by the server. " +
                     "Indicates the URL of a site trusted to make cross-domain requests to this server." +
                     "Allowed values: null (don't send the header), *, http://example.org (space separated if multiple sites)")]
        [DefaultValue((string)null)]
        [ConfigurationEntry("Raven/Server/AccessControlAllowOrigin")]
        [LegacyConfigurationEntry("Raven/AccessControlAllowOrigin")]
        public string AccessControlAllowOriginStringValue { get; set; }

        public HashSet<string> AccessControlAllowOrigin { get; set; }

        [Description("Determine the value of the Access-Control-Max-Age header sent by the server. " +
                     "Indicates how long (seconds) the browser should cache the Access Control settings. " +
                     "Ignored if AccessControlAllowOrigin is not specified.")]
        [DefaultValue("1728000" /* 20 days */)]
        [ConfigurationEntry("Raven/Server/AccessControlMaxAge")]
        [LegacyConfigurationEntry("Raven/AccessControlMaxAge")]
        public string AccessControlMaxAge { get; set; }

        [Description("Determine the value of the Access-Control-Allow-Methods header sent by the server." +
                     " Indicates which HTTP methods (verbs) are permitted for requests from allowed cross-domain origins." +
                     " Ignored if AccessControlAllowOrigin is not specified.")]
        [DefaultValue("PUT,PATCH,GET,DELETE,POST")]
        [ConfigurationEntry("Raven/Server/AccessControlAllowMethods")]
        [LegacyConfigurationEntry("Raven/AccessControlAllowMethods")]
        public string AccessControlAllowMethods { get; set; }

        [Description("Determine the value of the Access-Control-Request-Headers header sent by the server. " +
                     "Indicates which HTTP headers are permitted for requests from allowed cross-domain origins. " +
                     "Ignored if AccessControlAllowOrigin is not specified. " +
                     "Allowed values: null (allow whatever headers are being requested), HTTP header field name")]
        [DefaultValue(null)]
        [ConfigurationEntry("Raven/Server/AccessControlRequestHeaders")]
        [LegacyConfigurationEntry("Raven/AccessControlRequestHeaders")]
        public string AccessControlRequestHeaders { get; set; }

        [Description("The url to redirect the user to when then try to access the local studio")]
        [DefaultValue(null)]
        [ConfigurationEntry("Raven/Server/RedirectStudioUrl")]
        [LegacyConfigurationEntry("Raven/RedirectStudioUrl")]
        public string RedirectStudioUrl { get; set; }

        [Description("The server name")]
        [DefaultValue(null)]
        [ConfigurationEntry("Raven/Server/Name")]
        [LegacyConfigurationEntry("Raven/ServerName")]
        public string Name { get; set; }

        [Description("OAuth Token Certificate - Modulus")]
        [DefaultValue(null)]
        [ConfigurationEntry("Raven/OAuthTokenCertificateModulus")]
        public string OAuthTokenCertificateModulus { get; set; }

        [Description("OAuth Token Certificate - Exponent")]
        [DefaultValue(null)]
        [ConfigurationEntry("Raven/OAuthTokenCertificateExponent")]
        public string OAuthTokenCertificateExponent { get; set; }

        [Description("AnonymousUserAccessMode")]
        [DefaultValue(AnonymousUserAccessModeValues.Admin)]
        [ConfigurationEntry("Raven/AnonymousAccess")]
        public AnonymousUserAccessModeValues AnonymousUserAccessMode { get; internal set; }

        public IDisposable SetAccessMode(AnonymousUserAccessModeValues newVal)
        {
            var old = AnonymousUserAccessMode;
            AnonymousUserAccessMode = newVal;
            return new RestoreAccessMode(this, old);
        }

        public struct RestoreAccessMode : IDisposable
        {
            private readonly ServerConfiguration _parent;
            private readonly AnonymousUserAccessModeValues _valToRestore;

            public RestoreAccessMode(ServerConfiguration parent, AnonymousUserAccessModeValues valToRestore)
            {
                _parent = parent;
                _valToRestore = valToRestore;
            }

            public void Dispose()
            {
                _parent.AnonymousUserAccessMode = _valToRestore;
            }
        }

        public override void Initialize(IConfigurationRoot settings, IConfigurationRoot serverWideSettings, ResourceType type, string resourceName)
        {
            base.Initialize(settings, serverWideSettings, type, resourceName);

            AccessControlAllowOrigin = string.IsNullOrEmpty(AccessControlAllowOriginStringValue) ? new HashSet<string>() : new HashSet<string>(AccessControlAllowOriginStringValue.Split());
        }
    }
}