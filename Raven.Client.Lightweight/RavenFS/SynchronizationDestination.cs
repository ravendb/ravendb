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

        protected bool Equals(SynchronizationDestination other)
        {
            return string.Equals(serverUrl, other.serverUrl) && string.Equals(FileSystem, other.FileSystem);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != this.GetType()) return false;
            return Equals((SynchronizationDestination) obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                return ((serverUrl != null ? serverUrl.GetHashCode() : 0)*397) ^ (FileSystem != null ? FileSystem.GetHashCode() : 0);
            }
        }
    }
}