using System;
using System.Collections.Specialized;
using System.ComponentModel;
using Raven.Abstractions.Util.Encryptors;
using Raven.Database.Config.Attributes;

namespace Raven.Database.Config.Categories
{
    public class OAuthConfiguration : ConfigurationCategory
    {
        public OAuthConfiguration(string serverUrl)
        {
            TokenServer = serverUrl.EndsWith("/") ? serverUrl + "OAuth/API-Key" : serverUrl + "/OAuth/API-Key";
        }

        [Description("The url clients should use for authenticating when using OAuth mode.\r\nDefault: http://RavenDB-Server-Url/OAuth/API-Key - the internal OAuth server")]
        [DefaultValue(DefaultValueSetInConstructor)]
        [ConfigurationEntry("Raven/OAuth/TokenServer")]
        [ConfigurationEntry("Raven/OAuthTokenServer")]
        public string TokenServer { get; set; }

        [Description("The base 64 to the OAuth key use to communicate with the server. If no key is specified, one will be automatically created.\r\nDefault: none. ")]
        [DefaultValue(null)]
        [ConfigurationEntry("Raven/OAuth/TokenCertificate")]
        [ConfigurationEntry("Raven/OAuthTokenCertificate")]
        public string TokenCertificate { get; set; }

        public bool UseDefaultTokenServer { get; private set; }

        /// <summary>
        /// The certificate to use when verifying access token signatures for OAuth
        /// </summary>
        public byte[] TokenKey { get; set; }

        public override void Initialize(NameValueCollection settings)
        {
            base.Initialize(settings);

            TokenKey = GetOAuthKey();
            UseDefaultTokenServer = settings[RavenConfiguration.GetKey(x => x.OAuth.TokenServer)] == null;
        }

        private static readonly Lazy<byte[]> DefaultOauthKey = new Lazy<byte[]>(() =>
        {
            using (var rsa = Encryptor.Current.CreateAsymmetrical())
            {
                return rsa.ExportCspBlob(true);
            }
        });

        private byte[] GetOAuthKey()
        {
            if (string.IsNullOrEmpty(TokenCertificate) == false)
            {
                return Convert.FromBase64String(TokenCertificate);
            }
            return DefaultOauthKey.Value; // ensure we only create this once per process
        }
    }
}