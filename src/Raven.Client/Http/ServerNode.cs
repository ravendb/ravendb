using System;
using System.Collections.Generic;
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

        private static readonly int EmptyStringHash = string.Empty.GetHashCode();

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
            if (ReferenceEquals(null, obj))
                return false;
            if (ReferenceEquals(this, obj))
                return true;
            if (obj.GetType() != GetType())
                return false;
            return Equals((ServerNode)obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                return HashCombiner.CombineInline(Url?.GetHashCode() ?? EmptyStringHash, Database?.GetHashCode() ?? EmptyStringHash);
            }
        }

        private int _lastServerVersionCheck = 0;

        public string LastServerVersion { get; private set; }

        internal bool SupportsAtomicClusterWrites { get; set; }

        public bool ShouldUpdateServerVersion()
        {
            if (LastServerVersion == null || _lastServerVersionCheck > 100)
                return true;

            _lastServerVersionCheck++;
            return false;
        }

        public void UpdateServerVersion(string serverVersion)
        {
            LastServerVersion = serverVersion;
            _lastServerVersionCheck = 0;
            if (serverVersion != null && Version.TryParse(serverVersion, out var ver))
            {
                // 5.2 or higher
                SupportsAtomicClusterWrites = ver.Major == 5 && ver.Minor >= 2 ||
                                              ver.Major > 5;
            }
            else
            {
                SupportsAtomicClusterWrites = false;
            }
        }

        public void DiscardServerVersion()
        {
            LastServerVersion = null;
            _lastServerVersionCheck = 0;
        }

        internal static List<ServerNode> CreateFrom(ClusterTopology topology)
        {
            var nodes = new List<ServerNode>();
            if (topology == null)
                return nodes;

            foreach (var member in topology.Members)
            {
                nodes.Add(new ServerNode
                {
                    Url = member.Value,
                    ClusterTag = member.Key
                });
            }

            foreach (var watcher in topology.Watchers)
            {
                nodes.Add(new ServerNode
                {
                    Url = watcher.Value,
                    ClusterTag = watcher.Key
                });
            }

            return nodes;
        }
    }
}
