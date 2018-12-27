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

        public bool MatchCriteria(ScriptRunner.SingleRun run, DocumentsOperationContext context, object document, JsBlittableBridge.IResultModifier modifier, ref BlittableJsonReaderObject transformResult)
        {
            using (var result = run.Run(context, context, "execute", new[] { document }))
            {
                var resultAsBool = result.BooleanValue;
                if (resultAsBool != null)
                    return resultAsBool.Value;

                transformResult = result.TranslateToObject(context, modifier);
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
            if (ReferenceEquals(null, obj))
                return false;
            if (ReferenceEquals(this, obj))
                return true;
            if (obj.GetType() != GetType())
                return false;
            return Equals((SubscriptionPatchDocument)obj);
        }

        public bool Equals(SubscriptionPatchDocument other)
        {
            if (DeclaredFunctions.Length != other.DeclaredFunctions.Length)
                return false;

            if (Script != other.Script)
                return false;

            for (var index = 0; index < DeclaredFunctions.Length; index++)
            {
                if (DeclaredFunctions[index] != other.DeclaredFunctions[index])
                    return false;
            }

            return true;
        }

        public override int GetHashCode()
        {
            unchecked
            {
                int hashCode = Script != null ? Script.GetHashCode() : 0;
                foreach (var function in DeclaredFunctions)
                    hashCode = (hashCode * 397) ^ function.GetHashCode();

                return hashCode;
            }
        }
    }
}
