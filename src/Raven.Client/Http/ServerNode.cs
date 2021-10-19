using System;
using System.Collections.Generic;

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

        public string Url;
        public string Database;
        public string ClusterTag;
        public Role ServerRole;

        private bool Equals(ServerNode other)
        {
            return string.Equals(Url, other.Url) &&
                string.Equals(Database, other.Database);
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
                var hashCode = Url?.GetHashCode() ?? 0;
                hashCode = (hashCode * 397) ^ (Database?.GetHashCode() ?? 0);
                return hashCode;
            }
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
