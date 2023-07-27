using System;
using Raven.Client.Documents.Replication;

namespace Raven.Client.Documents.Operations.Replication
{
    public class InternalReplication : ReplicationNode
    {
        private string _nodeTag;
        public string NodeTag
        {
            get => _nodeTag;
            set
            {
                if (HashCodeSealed)
                    throw new InvalidOperationException(
                        $"NodeTag of 'InternalReplication' can't be modified after 'GetHashCode' was invoked, if you see this error it is likley a bug (NodeTag={_nodeTag} value={value} Url={Url}).");
                _nodeTag = value;
            }
        }
        public override string FromString()
        {
            return $"[{NodeTag}/{Url}]";
        }

        public override bool IsEqualTo(ReplicationNode other)
        {
            if (other is InternalReplication internalNode)
            {
                return base.IsEqualTo(internalNode) &&
                       string.Equals(Url, internalNode.Url, StringComparison.OrdinalIgnoreCase);
            }
            return false;
        }
        
        public override int GetHashCode()
        {
            unchecked
            {
                var hashCode = (int)CalculateStringHash(NodeTag);
                HashCodeSealed = true;
                return hashCode;
            }
        }
    }
}
