using Jint;
using Jint.Native;
using Jint.Runtime;
using Jint.Runtime.Interop;
using Jint.Runtime.References;
using Raven.Server.Documents.Indexes.Static.JavaScript;
using Raven.Client;

namespace Raven.Server.Documents.Patch
{
    public abstract class JintNullPropagationReferenceResolver : IReferenceResolver
    {
        private static readonly JsNumber _numberPositiveZero = new(0);
        private static readonly JsNumber _numberNegativeOne = new(-1);

        protected JsValue _selfInstance;
        protected BlittableObjectInstance _args;

        public bool TryUnresolvableReference(Engine engine, Reference reference, out JsValue value)
        {
            JsValue referencedName = reference.ReferencedName;

            if (referencedName.IsString() == false)
            {
                value = JsValue.Undefined;
                return false;
            }

            var name = referencedName.AsString();
            if (_args == null || name.StartsWith('$') == false)
            {
                if (name == "length")
                    value = _numberPositiveZero;
                else
                    value = reference.IsPropertyReference ? JsValue.Undefined : JsValue.Null;
                return true;
            }

            value = _args.Get(name.Substring(1));
            return true;
        }

        public virtual bool TryPropertyReference(Engine engine, Reference reference, ref JsValue value)
        {
            JsValue referencedName = reference.ReferencedName;

            if (referencedName.IsString() == false)
                return false;

            var name = referencedName.AsString();
            if (name == Constants.Documents.Metadata.Key &&
                reference.Base is BlittableObjectInstance boi)
            {
                value = engine.Invoke(ScriptRunner.SingleRun.GetMetadataMethod, boi);
                return true;
            }
            if (name == "reduce" &&
                value.IsArray() && value.AsArray().Length == 0)
            {
                value = JsValue.Null;
                return true;
            }

            if (value is DynamicJsNull)
            {
                value = DynamicJsNull.ImplicitNull;
                return true;
            }

            return value.IsNull() || value.IsUndefined();
        }

        public bool TryGetCallable(Engine engine, object callee, out JsValue value)
        {
            if (callee is Reference reference)
            {
                var baseValue = reference.Base;

                if (baseValue.IsUndefined() ||
                    baseValue.IsArray() && baseValue.AsArray().Length == 0)
                {
                    JsValue referencedName = reference.ReferencedName;

                    if (referencedName.IsString() == false)
                    {
                        value = JsValue.Undefined;
                        return false;
                    }

                    var name = referencedName.AsString();
                    switch (name)
                    {
                        case "reduce":
                        case "reduceRight":
                            value = new ClrFunctionInstance(engine, name, static (_, arguments) => arguments.At(1, JsValue.Null));
                            return true;
                        case "concat":
                            value = new ClrFunctionInstance(engine, name, static (_, arguments) => arguments.At(0));
                            return true;
                        case "includes":
                        case "some":
                            value = new ClrFunctionInstance(engine, name, static (_, _) => JsBoolean.False);
                            return true;
                        case "every":
                            value = new ClrFunctionInstance(engine, name, static (_, _) => JsBoolean.True);
                            return true;
                        case "findIndex":
                        case "findLastIndex":
                        case "indexOf":
                        case "lastIndexOf":
                            value = new ClrFunctionInstance(engine, name, static (_, _) => _numberNegativeOne);
                            return true;
                        case "filter":
                        case "flat":
                        case "flatMap":
                        case "map":
                        case "reverse":
                        case "slice":
                        case "sort":
                        case "splice":
                        case "toReversed":
                        case "toSorted":
                        case "toSpliced":
                        case "with":
                            value = new ClrFunctionInstance(engine, name, (_, _) => new JsArray(engine));
                            return true;
                    }
                }
                else if (baseValue.IsNull() == false)
                {
                    value = JsValue.Undefined;
                    return false;
                }
            }

            value = new ClrFunctionInstance(engine, "function", static (_, _) => JsValue.Undefined);
            return true;
        }

        public bool CheckCoercible(JsValue value)
        {
            return true;
        }
    }
}
