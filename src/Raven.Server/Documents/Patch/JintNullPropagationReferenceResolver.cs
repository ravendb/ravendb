using Jint;
using Jint.Native;
using Jint.Runtime;
using Jint.Runtime.Interop;
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

            if (value.IsNull() || value.IsUndefined())
            {
                // Preserve the legacy "length of a missing/undefined value is 0" convenience.
                // In Jint 4.4.2 this was handled by TryUnresolvableReference (name == "length" => 0),
                // but since Jint 4.6.x property access on an undefined base routes through
                // TryPropertyReference instead (see the routing note below), so we handle it here.
                // Restricted to undefined (not null) to match the old behavior exactly: only an
                // undefined base was an unresolvable reference, whereas null bases propagated to null.
                if (name == "length" && value.IsUndefined())
                {
                    value = _numberPositiveZero;
                    return true;
                }

                // Normalize undefined to null for null propagation.
                // In Jint 4.6.x, Reference.IsUnresolvableReference changed from checking
                // base._type == Undefined to ReferenceEquals(base, Unresolvable). This means
                // property access on undefined values (e.g. r.FirstName where r is undefined)
                // now routes through TryPropertyReference instead of TryUnresolvableReference.
                // We must normalize to null here to preserve backward compatibility, matching
                // the old TryUnresolvableReference behavior that returned JsValue.Null.
                value = JsValue.Null;
                return true;
            }

            return false;
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
                            value = new ClrFunction(engine, name, static (_, arguments) => arguments.At(1, JsValue.Null));
                            return true;
                        case "concat":
                            value = new ClrFunction(engine, name, static (_, arguments) => arguments.At(0));
                            return true;
                        case "includes":
                        case "some":
                            value = new ClrFunction(engine, name, static (_, _) => JsBoolean.False);
                            return true;
                        case "every":
                            value = new ClrFunction(engine, name, static (_, _) => JsBoolean.True);
                            return true;
                        case "findIndex":
                        case "findLastIndex":
                        case "indexOf":
                        case "lastIndexOf":
                            value = new ClrFunction(engine, name, static (_, _) => _numberNegativeOne);
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
                            value = new ClrFunction(engine, name, (_, _) => new JsArray(engine));
                            return true;
                    }
                }
                else if (baseValue.IsNull() == false)
                {
                    value = JsValue.Undefined;
                    return false;
                }
            }

            value = new ClrFunction(engine, "function", static (_, _) => JsValue.Undefined);
            return true;
        }

        public bool CheckCoercible(JsValue value)
        {
            return true;
        }
    }
}
