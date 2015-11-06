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

        [DefaultValue(DefaultValueSetInConstructor)]
        [ConfigurationEntry("Raven/OAuthTokenServer")]
        public string TokenServer { get; set; }

        public bool UseDefaultTokenServer { get; private set; }

        /// <summary>
        /// The certificate to use when verifying access token signatures for OAuth
        /// </summary>
        public byte[] TokenKey { get; set; }

        public override void Initialize(NameValueCollection settings)
        {
            base.Initialize(settings);

            TokenKey = GetOAuthKey(settings);
            UseDefaultTokenServer = settings[InMemoryRavenConfiguration.GetKey(x => x.OAuth.TokenServer)] == null;
        }

        private static readonly Lazy<byte[]> DefaultOauthKey = new Lazy<byte[]>(() =>
        {
            using (var rsa = Encryptor.Current.CreateAsymmetrical())
            {
                return rsa.ExportCspBlob(true);
            }
        });

        private byte[] GetOAuthKey(NameValueCollection settings)
        {
            var key = settings["Raven/OAuthTokenCertificate"];
            if (string.IsNullOrEmpty(key) == false)
            {
                return Convert.FromBase64String(key);
            }
            return DefaultOauthKey.Value; // ensure we only create this once per process
        }
    }
}