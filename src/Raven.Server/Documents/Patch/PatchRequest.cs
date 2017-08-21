using System.IO;
using System.Text.RegularExpressions;
using Sparrow.Json;

namespace Raven.Server.Documents.Patch
{
    /// <summary>
    /// An advanced patch request for a specified document (using JavaScript)
    /// </summary>
    public class PatchRequest : ScriptRunnerCache.Key
    {
        /// <summary>
        /// JavaScript function to use to patch a document
        /// </summary>
        /// <value>The type.</value>
        public readonly string Script;

        /// <summary>
        /// Additional arguments passed to JavaScript function from Script.
        /// </summary>
        public BlittableJsonReaderObject Values;

        public PatchRequest(string script)
        {
            Script = script;
        }

        protected bool Equals(PatchRequest other)
        {
            if (other == null)
                return false;
            return string.Equals(Script, other.Script);
        }

        public override string GenerateScript()
        {
            return $@"
function execute(doc){{ 
    var actual = function() {{ 


{Script}


    }};
    actual.call(doc);
    return doc;
}}";
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            return Equals(obj as PatchRequest);
        }

        public override int GetHashCode()
        {
            // to make sure that different keys has different hashes for the same script
            return Script.GetHashCode() ^ 42;
        }

        public static PatchRequest Parse(BlittableJsonReaderObject input)
        {
            if (input.TryGet("Script", out string script) == false || script == null)
                throw new InvalidDataException("Missing 'Script' property on 'Patch'");

            var patch = new PatchRequest(script);

            if (input.TryGet("Values", out BlittableJsonReaderObject values))
                patch.Values = values;

            return patch;
        }
    }
}
