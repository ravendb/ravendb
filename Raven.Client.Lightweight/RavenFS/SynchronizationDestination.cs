// -----------------------------------------------------------------------
//  <copyright file="SynchronizationDestination.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

namespace Raven.Client.RavenFS
{
    public class SynchronizationDestination
    {
        private string serverUrl;

        public string ServerUrl
        {
            get { return serverUrl; }
            set
            {
                serverUrl = value.EndsWith("/") ? value.Substring(0, value.Length - 1) : value;
            }
        }

        public string FileSystem { get; set; }

        public string FileSystemUrl
        {
            get { return string.Format("{0}/ravenfs/{1}", ServerUrl, FileSystem); }
        }

        public string Username { get; set; }

        public string Password { get; set; }

        public string Domain { get; set; }

        public string ApiKey { get; set; }

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