using System;
using Jint;
using Jint.Native;
using Jint.Runtime.Interop;
using Jint.Runtime.References;

namespace Raven.Server.Documents.Patch
{
    public abstract class JintNullPropagationReferenceResolver : IReferenceResolver
    {
        public virtual bool TryUnresolvableReference(Engine engine, Reference reference, out JsValue value)
        {
            value = reference.GetReferencedName() == "length" ? 0 : Null.Instance;
            return true;
        }

        public virtual bool TryPropertyReference(Engine engine, Reference reference, ref JsValue value)
        {
            if (reference.GetReferencedName() == "reduce" &&
                value.IsArray() && value.AsArray().GetLength() == 0)
            {
                value = Null.Instance;
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
                    baseValue.IsArray() && baseValue.AsArray().GetLength() == 0)
                {
                    var name = reference.GetReferencedName();
                    switch (name)
                    {
                        case "reduce":
                            value = new ClrFunctionInstance(engine, (thisObj, values) => values.Length > 1 ? values[1] : thisObj);
                            return true;
                        case "concat":
                            value = new ClrFunctionInstance(engine, (thisObj, values) => values[0]);
                            return true;
                        case "some":
                        case "includes":
                            value = new ClrFunctionInstance(engine, (thisObj, values) => false);
                            return true;
                        case "every":
                            value = new ClrFunctionInstance(engine, (thisObj, values) => true);
                            return true;
                        case "map":
                        case "filter":
                        case "reverse":
                            value = new ClrFunctionInstance(engine, (thisObj, values) => engine.Array.Construct(Array.Empty<JsValue>()));
                            return true;
                    }
                }
            }

            value = new ClrFunctionInstance(engine, (thisObj, values) => thisObj);
            return true;
        }

        public bool CheckCoercible(JsValue value)
        {
            return true;
        }
    }
}
