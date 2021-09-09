using System;
using V8.Net;

namespace Raven.Server.Documents.Indexes.Static.JavaScript
{
    public sealed class DynamicJsNull : IEquatable<InternalHandle>, IEquatable<DynamicJsNull>
    {
        public readonly bool IsExplicitNull;
        private InternalHandle _handle;

        public DynamicJsNull(V8Engine engine, bool isExplicitNull) : base()
        {
            IsExplicitNull = isExplicitNull;
            _handle = engine.CreateNullValue();
        }

        ~DynamicJsNull()
        {
            _handle.Dispose();
        }

        public override string ToString()
        {
            return "null";
        }

        public InternalHandle CreateHandle()
        {
            return new InternalHandle(ref _handle, true);
        }

        public bool Equals(InternalHandle jsOther)
        {
            if (jsOther.IsNull)
                return _handle.Equals(jsOther);

            return false;
        }

        public bool Equals(DynamicJsNull other)
        {
            if (ReferenceEquals(null, other))
            {
                return false;
            }

            return true; // isExplicitNull == other.isExplicitNull
        }
    }
}
