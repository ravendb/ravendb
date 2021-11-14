using Jint;
using Jint.Native.Object;
using Jint.Runtime.Descriptors;

namespace Raven.Server.Documents.Indexes.Static.JavaScript.Jint
{
    public abstract class ObjectInstanceBaseJint : ObjectInstance
    {
        protected static readonly PropertyDescriptor ImplicitNull = new PropertyDescriptor(DynamicJsNullJint.ImplicitNullJint, writable: false, enumerable: false, configurable: false);

        protected ObjectInstanceBaseJint(Engine engine) : base(engine)
        {
            SetPrototypeOf(engine.Realm.Intrinsics.Object.PrototypeObject);
        }
    }
}
