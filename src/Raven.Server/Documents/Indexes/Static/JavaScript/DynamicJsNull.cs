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

        public override string ToString()
        {
            return "null";
        }

        public override bool Equals(object other)
        {
            if (ReferenceEquals(this, other))
                return true;

            if (other is InternalHandle jsOther)
                return jsOther.IsNull;

            if (other is Handle hOther)
            {
                if (ReferenceEquals(null, hOther))
                    return false;

                if (hOther._.IsNull)
                    return true;

                if (hOther is DynamicJsNull dynamicJsNull)
                    return true;
            }

            return false;
        }
    }
}
