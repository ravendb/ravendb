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
function execute(doc, args){{ 
    var actual = function(args) {{ 


{Script}


    }};
    actual.call(doc, args);
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

        public static PatchRequest Parse(BlittableJsonReaderObject input, out BlittableJsonReaderObject args)
        {
            if (input.TryGet("Script", out string script) == false || script == null)
                throw new InvalidDataException("Missing 'Script' property on 'Patch'");

            var patch = new PatchRequest(script);

            input.TryGet("Values", out args);

            return patch;
        }
    }
}
