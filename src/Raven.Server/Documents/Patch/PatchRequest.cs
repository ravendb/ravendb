using System;
using System.IO;
using System.Text.RegularExpressions;
using Sparrow.Json;

namespace Raven.Server.Documents.Patch
{
    public enum PatchRequestType
    {
        None,
        Patch,
        Conflict,
        Subscription,
        SqlEtl,
        RavenEtl,
        Smuggler
    }

    /// <summary>
    /// An advanced patch request for a specified document (using JavaScript)
    /// </summary>
    public class PatchRequest : ScriptRunnerCache.Key
    {
        /// <summary>
        /// JavaScript function to use to patch a document
        /// </summary>
        public readonly string Script;

        public readonly PatchRequestType Type;

        public PatchRequest(string script, PatchRequestType type)
        {
            Script = script;
            Type = type;
        }

        protected bool Equals(PatchRequest other)
        {
            return string.Equals(Script, other.Script) && Type == other.Type;
        }

        public override string GenerateScript()
        {
            switch (Type)
            {
                case PatchRequestType.None:
                case PatchRequestType.SqlEtl:
                case PatchRequestType.Smuggler:
                case PatchRequestType.RavenEtl:
                    // modify and return the document
                case PatchRequestType.Patch:
                    return $@"
function execute(doc, args){{ 
    var actual = function(args) {{ 


{Script}


    }};
    actual.call(doc, args);
    return doc;
}}";
                    // get the document and return the result of the script
                    // and without arguments
                case PatchRequestType.Subscription:
                    return $@"
function execute(doc){{ 
    var actual = function() {{ 


{Script}


    }};
    return actual.call(doc);
}}";
                case PatchRequestType.Conflict:
                    return $@"
function resolve(docs, hasTombstone, resolveToTombstone){{ 

{Script}

}}";
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj))
                return false;
            if (ReferenceEquals(this, obj))
                return true;
            if (obj.GetType() != this.GetType())
                return false;
            return Equals((PatchRequest)obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                int hashCode = 0;
                hashCode = (hashCode * 397) ^ (Script != null ? Script.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (int)Type;
                return hashCode;
            }
        }

        public static PatchRequest Parse(BlittableJsonReaderObject input, out BlittableJsonReaderObject args)
        {
            if (input.TryGet("Script", out string script) == false || script == null)
                throw new InvalidDataException("Missing 'Script' property on 'Patch'");

            var patch = new PatchRequest(script, PatchRequestType.Patch);

            input.TryGet("Values", out args);

            return patch;
        }
    }
}
