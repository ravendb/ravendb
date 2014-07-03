using Raven.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Raven.Abstractions.FileSystem
{
    public class DirectoryHeader
    {
        public RavenJObject Metadata { get; set; }

        public string Name { get; set; }
        public string Path { get; private set; }

        public int Files { get; set; }

        protected bool Equals(DirectoryHeader other)
        {
            return string.Equals(Name, other.Name) && Files == other.Files && RavenJObject.Equals(Metadata, other.Metadata);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != GetType()) return false;
            return Equals((DirectoryHeader)obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                var hashCode = (Name != null ? Name.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ Files.GetHashCode();
                hashCode = (hashCode * 397) ^ (Metadata != null ? Metadata.GetHashCode() : 0);
                return hashCode;
            }
        }
    }
}
