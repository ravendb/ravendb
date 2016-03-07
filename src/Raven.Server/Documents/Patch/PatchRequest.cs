using System.Collections.Generic;
using System.Linq;

namespace Raven.Server.Documents.Patch
{
    /// <summary>
    /// An advanced patch request for a specified document (using JavaScript)
    /// </summary>
    public class PatchRequest
    {
        /// <summary>
        /// JavaScript function to use to patch a document
        /// </summary>
        /// <value>The type.</value>
        public string Script;

        /// <summary>
        /// Additional arguments passed to JavaScript function from Script.
        /// </summary>
        public Dictionary<string, object> Values;

        protected bool Equals(PatchRequest other)
        {
            if(other == null)
                return false;
            return string.Equals(Script, other.Script) && (Values?.Keys.SequenceEqual(other.Values.Keys) ?? true);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            return Equals(obj as PatchRequest);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                if (Values == null)
                    return Script.GetHashCode();
                return Values.Keys.Aggregate(Script.GetHashCode()*397, (i, s) => i*397 ^ s.GetHashCode());
            }
        }
    }
}
