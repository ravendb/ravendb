using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Rachis.Communication
{
    //TODO: Adjust this class to support TCP connection needs
    public class NodeConnectionInfo
    {

        public string Name { get; set; }

        public bool IsNoneVoter { get; set; }

        public string ConnectionIdentifier { get; set; } //TODO:replace this with a socket address

        public override string ToString()
        {
            return Name;
        }

        protected bool Equals(NodeConnectionInfo other)
        {
            return string.Equals(Name, other.Name);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != GetType()) return false;
            return Equals((NodeConnectionInfo)obj);
        }


        public static bool operator ==(NodeConnectionInfo left, NodeConnectionInfo right)
        {
            return Equals(left, right);
        }

        public static bool operator !=(NodeConnectionInfo left, NodeConnectionInfo right)
        {
            return !Equals(left, right);
        }

    }
}
