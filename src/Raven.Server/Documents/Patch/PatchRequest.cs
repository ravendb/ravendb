using System;
using System.Collections.Generic;
using System.IO;
using Sparrow;
using Raven.Server.Documents.Queries.AST;
using Sparrow.Json;

namespace Raven.Server.Documents.Patch
{
    public enum PatchRequestType
    {
        None,
        Patch,
        Conflict,        
        SqlEtl,
        ElasticsearchEtl,
        RavenEtl,
        OlapEtl,
        Smuggler,
        EtlBehaviorFunctions
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

        private readonly Dictionary<string, DeclaredFunction> _functions;

        public PatchRequest(string script, PatchRequestType type, Dictionary<string, DeclaredFunction> functions = null)
        {
            Script = script;
            _functions = functions;
            Type = type;
        }

        protected bool Equals(PatchRequest other)
        {
            if ((string.Equals(Script, other.Script) && Type == other.Type) == false)
                return false;

            if (_functions != null)
            {
                foreach (var function in _functions)
                {
                    if (other._functions.TryGetValue(function.Key, out var otherVal) == false
                        || function.Value.FunctionText != otherVal.FunctionText)
                        return false;
                }
            }

            return true;

        }

        public override void GenerateScript(ScriptRunner runner)
        {
            if (_functions!= null)
            {
                foreach(var function in _functions)
                {
                    runner.AddScript(function.Value.FunctionText);
                }
            }
            runner.AddScript(GenerateRootScript());
        }

        private string GenerateRootScript()
        {
            switch (Type)
            {
                case PatchRequestType.None:
                case PatchRequestType.SqlEtl:
                case PatchRequestType.ElasticsearchEtl:
                case PatchRequestType.Smuggler:
                case PatchRequestType.RavenEtl:
                case PatchRequestType.OlapEtl:
                // modify and return the document
                case PatchRequestType.Patch:
                    return $@"
 function __actual_func(args) {{ 
Raven_ExplodeArgs(this, args);
{Script}

}};

function execute(doc, args){{ 
    __actual_func.call(doc, args);
    return doc;
}}";
                
                case PatchRequestType.Conflict:
                    return $@"
function resolve(docs, hasTombstone, resolveToTombstone){{ 

{Script}

}}";
                case PatchRequestType.EtlBehaviorFunctions:
                    return Script;
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
                int hashCode = Script?.GetHashCode() ?? 0;
                hashCode = (hashCode * 397) ^ (int)Type;
                if (_functions != null)
                {
                    foreach (var function in _functions)
                    {
                        hashCode = (hashCode * 397) ^ (function.Value.FunctionText.GetHashCode());
                    }
                }
                return hashCode;
            }
        }

        public static PatchRequest Parse(BlittableJsonReaderObject input, out BlittableJsonReaderObject args)
        {
            
            if (input.TryGet("Script", out string script) == false || script == null)
                throw new InvalidDataException("Missing 'Script' property on 'Patch'");

            // todo: maybe support receiving functions here? not sure, because this function is for single-document patches, which is not in RQL notation in the first place
            var patch = new PatchRequest(script,PatchRequestType.Patch);

            input.TryGet("Values", out args);

            return patch;
        }
    }
}
