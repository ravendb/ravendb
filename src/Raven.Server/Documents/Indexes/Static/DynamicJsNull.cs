using System;
using V8.Net;

namespace Raven.Server.Documents.Indexes.Static.JavaScript
{
    public sealed class DynamicJsNull : Handle, IEquatable<InternalHandle>, IEquatable<DynamicJsNull>
    {
        public static DynamicJsNull ImplicitNull = new DynamicJsNull(isExplicitNull: false);

        public static DynamicJsNull ExplicitNull = new DynamicJsNull(isExplicitNull: true);

        public readonly bool IsExplicitNull;

        private DynamicJsNull(bool isExplicitNull) : base(InternalHandle.Empty)
        {
            IsExplicitNull = isExplicitNull;
        }

        public object ToObject()
        {
            return null;
        }

        public string ToString()
        {
            return "null";
        }

        public bool Equals(InternalHandle other)
        {
            if (ReferenceEquals(this, other))
            {
                return true;
            }

            if (other.IsNull)
                return true;

            if (other.BoundObject is DynamicJsNull dynamicJsNull)
                return Equals(dynamicJsNull);
            return false;
        }

        public bool Equals(DynamicJsNull other)
        {
            if (ReferenceEquals(null, other))
            {
                return false;
            }

            return true;
        }
    }
}
