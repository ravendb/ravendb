using Jint;
using Jint.Native.Object;
using Jint.Runtime.Descriptors;
using Raven.Server.Documents.Patch;
using Raven.Server.Documents.Patch.Jint;

namespace Raven.Server.Documents.Indexes.Static.JavaScript.Jint
{
    public abstract class ObjectInstanceBaseJint : ObjectInstance/*,IObjectInstance<JsHandleJint> */
    {
        protected static readonly PropertyDescriptor ImplicitNull = new PropertyDescriptor(DynamicJsNullJint.ImplicitNullJint, writable: false, enumerable: false, configurable: false);

        protected ObjectInstanceBaseJint(Engine engine) : base(engine)
        {
            SetPrototypeOf(engine.Realm.Intrinsics.Object.PrototypeObject);
        }
    }
}
