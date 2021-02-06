using System;
using static Sparrow.Hashing;

namespace Raven.Client.Http
{
    public class ServerNode
    {
        [Flags]
        public enum Role
        {
            None = 0,
            Promotable = 1,
            Member = 2,
            Rehab = 4
        }

        private static int _emptyStringHash = string.Empty.GetHashCode();

        public string Url;
        public string Database;

        public string ClusterTag;
        public Role ServerRole;

        private bool Equals(ServerNode other)
        {
            return string.Equals(Url, other.Url) && string.Equals(Database, other.Database);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != GetType()) return false;
            return Equals((ServerNode)obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                return HashCombiner.CombineInline(Url?.GetHashCode() ?? _emptyStringHash, Database?.GetHashCode() ?? _emptyStringHash);
            }
        }

        private int _lastServerVersionCheck = 0;
        
        public string LastServerVersion { get; private set; }
        
        public bool ShouldUpdateServerVersion()
        {            
            if (LastServerVersion == null || _lastServerVersionCheck > 100)
                return true;

            _lastServerVersionCheck++;
            return false;
        }

        public void UpdateServerVersion (string serverVersion)
        {
            LastServerVersion = serverVersion;
            _lastServerVersionCheck = 0;            
        }

        public void DiscardServerVersion()
        {
            LastServerVersion = null;
            _lastServerVersionCheck = 0;
        }

        
    }
}
