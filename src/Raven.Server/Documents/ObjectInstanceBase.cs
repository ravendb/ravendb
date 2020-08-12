using Jint;
using Jint.Native.Object;
using Jint.Runtime.Descriptors;
using Raven.Server.Documents.Patch;

namespace Raven.Server.Documents
{
    public abstract class ObjectInstanceBase : ObjectInstance
    {
        protected static readonly PropertyDescriptor ImplicitNull = new PropertyDescriptor(DynamicJsNull.ImplicitNull, writable: false, enumerable: false, configurable: false);

        protected ObjectInstanceBase(Engine engine) : base(engine)
        {
            SetPrototypeOf(engine.Object.PrototypeObject);
        }
    }
}
