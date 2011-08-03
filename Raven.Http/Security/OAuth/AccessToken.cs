using System;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using Newtonsoft.Json;
using Raven.Json.Linq;

namespace Raven.Http.Security.OAuth
{
    public class AccessToken
    {
        public string Body { get; set; }
        public string Signature { get; set; }
        
        public bool MatchesSignature(string certPath)
        {
            var cert = new X509Certificate2(certPath);
            var csp = (RSACryptoServiceProvider)cert.PublicKey.Key;

            var signatureData = Convert.FromBase64String(Signature);
            var bodyData = Encoding.Unicode.GetBytes(Body);

			using (var hasher = new SHA1Managed())
			{
				var hash = hasher.ComputeHash(bodyData);

				return csp.VerifyHash(hash, CryptoConfig.MapNameToOID("SHA1"), signatureData);
			}
        }

        public bool TryParseBody(out AccessTokenBody body)
        {
            try
            {
                body = JsonConvert.DeserializeObject<AccessTokenBody>(Body);
                return true;
            }
            catch
            {
                body = null;
                return false;
            }
        }

        public static bool TryParse(string token, out AccessToken accessToken)
        {
            try
            {
                accessToken = JsonConvert.DeserializeObject<AccessToken>(token);
                return true;
            }
            catch
            {
                accessToken = null;
                return false;
            }
        }

        public static AccessToken Create(string certPath, string certPassword, string userId, string[] databases)
        {
            var issued = (DateTime.UtcNow - DateTime.MinValue).TotalMilliseconds;
            
            var body = RavenJObject.FromObject(new { UserId = userId, AuthorizedDatabases = databases ?? new string[0], Issued = issued })
                    .ToString(Formatting.None);

            var signature = Sign(body, certPath, certPassword);

            return new AccessToken { Body = body, Signature = signature };
        }

        static string Sign(string body, string certPath, string password)
        {
            var cert = new X509Certificate2(certPath, password);
            var csp = (RSACryptoServiceProvider)cert.PrivateKey;

            var data = Encoding.Unicode.GetBytes(body);
			using (var hasher = new SHA1Managed())
			{
				var hash = hasher.ComputeHash(data);
				return Convert.ToBase64String(csp.SignHash(hash, CryptoConfig.MapNameToOID("SHA1")));
			}
        }

        public string Serialize()
        {
            return RavenJObject.FromObject(this).ToString(Formatting.None);
        }

    }
}