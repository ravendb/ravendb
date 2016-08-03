using System.Net;
using Raven.Imports.Newtonsoft.Json;

namespace Raven.Abstractions.FileSystem
{
    public class SynchronizationDestination
    {
        private string serverUrl;

        public string ServerUrl
        {
            get { return serverUrl; }
            set
            {
                var canonicalUrl = value.TrimEnd('/');

                serverUrl = canonicalUrl;
                if (canonicalUrl.EndsWith("/fs"))
                    serverUrl = canonicalUrl.Substring(0, value.Length - 3);
            }
        }

        public string FileSystem { get; set; }

        public string Url => $"{ServerUrl}/fs/{FileSystem}";

        public string Username { get; set; }

        public string Password { get; set; }

        public string Domain { get; set; }

        public string ApiKey { get; set; }

        public string AuthenticationScheme { get; set; }

        public bool Enabled { get; set; }

        public SynchronizationDestination ()
        {
            this.Enabled = true;
        }

        [JsonIgnore]
        public ICredentials Credentials
        {
            get
            {
                if (string.IsNullOrEmpty(Username) == false)
                {
                    return string.IsNullOrEmpty(Domain)
                                      ? new NetworkCredential(Username, Password)
                                      : new NetworkCredential(Username, Password, Domain);
                }
                return null;
            }
        }

        protected bool Equals(SynchronizationDestination other)
        {
            return string.Equals(serverUrl, other.serverUrl) && string.Equals(ApiKey, other.ApiKey) && string.Equals(Domain, other.Domain) &&
                string.Equals(Password, other.Password) && string.Equals(Username, other.Username) && string.Equals(FileSystem, other.FileSystem);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != this.GetType()) return false;
            return Equals((SynchronizationDestination)obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                var hashCode = (serverUrl != null ? serverUrl.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (ApiKey != null ? ApiKey.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (Domain != null ? Domain.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (Password != null ? Password.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (Username != null ? Username.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (FileSystem != null ? FileSystem.GetHashCode() : 0);
                return hashCode;
            }
        }
    }
}
