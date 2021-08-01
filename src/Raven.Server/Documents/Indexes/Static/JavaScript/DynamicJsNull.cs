using System;
using V8.Net;

namespace Raven.Server.Documents.Indexes.Static.JavaScript
{
    public sealed class DynamicJsNull : Handle, IEquatable<InternalHandle>, IEquatable<Handle>
    {
        public static DynamicJsNull ImplicitNull = new DynamicJsNull(isExplicitNull: false);

        public static DynamicJsNull ExplicitNull = new DynamicJsNull(isExplicitNull: true);

        public readonly bool IsExplicitNull;

        private DynamicJsNull(bool isExplicitNull) : base()
        {
            IsExplicitNull = isExplicitNull;
        }

        public override string ToString()
        {
            return "null";
        }

        public override bool Equals(object other)
        {
            if (ReferenceEquals(this, other))
                return true;

            if (other is InternalHandle jsOther)
                return Equals(jsOther);

            if (other is Handle hOther)
                return Equals(hOther);

            return false;
        }

        public bool Equals(InternalHandle jsOther)
        {
            return jsOther.IsNull;
        }

        public bool Equals(Handle other)
        {
            if (ReferenceEquals(null, other))
                return false;

            if (other._.IsNull)
                return true;

            if (other is DynamicJsNull dynamicJsNull)
                return true;

            return false;
        }
    }
}
