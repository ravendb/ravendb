using System.IO;
using Sparrow.Json;

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
        public BlittableJsonReaderObject Values;

        protected bool Equals(PatchRequest other)
        {
            if(other == null)
                return false;
            return string.Equals(Script, other.Script);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            return Equals(obj as PatchRequest);
        }

        public override int GetHashCode()
        {
            return Script.GetHashCode();
        }

        public static PatchRequest Parse(BlittableJsonReaderObject input)
        {
            var patch = new PatchRequest();
            if (input.TryGet("Script", out patch.Script) == false)
                throw new InvalidDataException("Missing 'Script' property on 'Patch'");

            BlittableJsonReaderObject values;
            if (input.TryGet("Values", out values))
                patch.Values = values;

            return patch;
        }
    }
}
