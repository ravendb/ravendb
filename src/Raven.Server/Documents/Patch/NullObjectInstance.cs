using Jint;
using Jint.Native.Object;
using Jint.Runtime.Descriptors;
using Jint.Runtime.Descriptors.Specialized;

namespace Raven.Server.Documents.Patch
{
    public class NullObjectInstance : ObjectInstance
    {
        public NullObjectInstance(Engine engine) : base(engine)
        {
        }

        public override PropertyDescriptor GetOwnProperty(string propertyName)
        {
            return new ClrAccessDescriptor(Engine, self => this, (self, value) => { });
        }
    }
}
