using Raven.Server.Documents.Patch;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Subscriptions
{
    public class SubscriptionPatchDocument : ScriptRunnerCache.Key
    {
        public readonly string Script;
        public readonly string[] DeclaredFunctions;


        public SubscriptionPatchDocument(string script, string[] declaredFunctions)
        {
            Script = script;
            DeclaredFunctions = declaredFunctions;
        }

        public bool MatchCriteria(ScriptRunner.SingleRun run, DocumentsOperationContext context, object document, ref BlittableJsonReaderObject transformResult)
        {
            using (var result = run.Run(context, "execute", new[] { document }))
            {
                var resultAsBool = result.BooleanValue;
                if (resultAsBool != null)
                    return resultAsBool.Value;

                transformResult = result.TranslateToObject(context);
                return transformResult != null;
            }
        }

        public override void GenerateScript(ScriptRunner runner)
        {
            foreach (var script in DeclaredFunctions)
            {
                runner.AddScript(script);
            }
            runner.AddScript($@"
function __actual_func(args) {{ 

{Script}

}};
function execute(doc, args){{ 
    return __actual_func.call(doc, args);
}}");
        }

        public override bool Equals(object obj)
        {
            if (obj is SubscriptionPatchDocument other)
                return Equals(other);
            return false;
        }

        public bool Equals(SubscriptionPatchDocument obj)
        {
            if (DeclaredFunctions.Length != obj.DeclaredFunctions.Length)
                return false;

            for (var index = 0; index < DeclaredFunctions.Length; index++)
            {
                if (DeclaredFunctions[index] != obj.DeclaredFunctions[index])
                    return false;
            }

            return obj.Script != Script;
        }

        public override int GetHashCode()
        {
            unchecked
            {
                int hashCode = Script.GetHashCode();
                foreach (var function in DeclaredFunctions)
                {
                    hashCode = (hashCode * 397) ^ (function.GetHashCode());
                }
                return hashCode;
            }
        }
    }
}
