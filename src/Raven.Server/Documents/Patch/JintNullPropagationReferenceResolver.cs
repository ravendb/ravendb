using System;
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

        protected JsValue _selfInstance;
        protected BlittableObjectInstance _args;

        public virtual bool TryUnresolvableReference(Engine engine, Reference reference, out JsValue value)
        {
            JsValue referencedName = reference.GetReferencedName();

            if (referencedName.IsString() == false)
            {
                value = JsValue.Undefined;
                return false;
            }

            var name = referencedName.AsString();
            if (_args == null || name.StartsWith('$') == false)
            {
                value = name == "length" ? _numberPositiveZero : JsValue.Null;
                return true;
            }

            value = _args.Get(name.Substring(1));
            return true;
        }

        public virtual bool TryPropertyReference(Engine engine, Reference reference, ref JsValue value)
        {
            JsValue referencedName = reference.GetReferencedName();

            if (referencedName.IsString() == false)
                return false;

            var name = referencedName.AsString();
            if (name == Constants.Documents.Metadata.Key &&
                reference.GetBase() is BlittableObjectInstance boi)
            {
                value = engine.Invoke(ScriptRunner.SingleRun.GetMetadataMethod, boi);
                return true;
            }
            if (name == "reduce" &&
                value.IsArray() && value.AsArray().Length == 0)
            {
                value = Null.Instance;
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
                var baseValue = reference.GetBase();

                if (baseValue.IsUndefined() ||
                    baseValue.IsArray() && baseValue.AsArray().Length == 0)
                {
                    JsValue referencedName = reference.GetReferencedName();

                    if (referencedName.IsString() == false)
                    {
                        value = JsValue.Undefined;
                        return false;
                    }

                    var name = referencedName.AsString();
                    switch (name)
                    {
                        case "reduce":
                            value = new ClrFunctionInstance(engine, "reduce", static (thisObj, values) => values.At(1, JsValue.Null));
                            return true;
                        case "concat":
                            value = new ClrFunctionInstance(engine, "concat", static (thisObj, values) => values.At(0));
                            return true;
                        case "some":
                        case "includes":
                            value = new ClrFunctionInstance(engine, "some", static (thisObj, values) => JsBoolean.False);
                            return true;
                        case "every":
                            value = new ClrFunctionInstance(engine, "every", static (thisObj, values) => JsBoolean.True);
                            return true;
                        case "map":
                        case "filter":
                        case "reverse":
                            value = new ClrFunctionInstance(engine, "map", (thisObj, values) => engine.Array.Construct(Array.Empty<JsValue>()));
                            return true;
                    }
                }
            }

            value = new ClrFunctionInstance(engine, "function", static (thisObj, values) => thisObj);
            return true;
        }

        public bool CheckCoercible(JsValue value)
        {
            return true;
        }
    }
}
