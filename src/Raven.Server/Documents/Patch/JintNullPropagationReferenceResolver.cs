using System;
using Jint;
using Jint.Native;
using Jint.Runtime.Interop;
using Jint.Runtime.References;

namespace Raven.Server.Documents.Patch
{
    public abstract class JintNullPropagationReferenceResolver : IReferenceResolver
    {
        protected JsValue _selfInstance;
        protected BlittableObjectInstance _args;

        public virtual bool TryUnresolvableReference(Engine engine, Reference reference, out JsValue value)
        {
            var name = reference.GetReferencedName();
            if (_args == null || name == null || name.StartsWith('$') == false)
            {
                value = name == "length" ? 0 : Null.Instance;
                return true;
            }

            value = _args.Get(name.Substring(1));
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
                            value = new ClrFunctionInstance(engine, (thisObj, values) => values.Length > 1 ? values[1] : JsValue.Null);
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
