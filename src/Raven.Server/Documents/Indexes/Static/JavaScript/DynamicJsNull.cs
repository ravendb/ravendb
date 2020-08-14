using System;
using Jint.Native;
using Jint.Runtime;

namespace Raven.Server.Documents.Indexes.Static.JavaScript
{
    public sealed class DynamicJsNull : JsValue, IEquatable<JsNull>, IEquatable<DynamicJsNull>
    {
        public static DynamicJsNull ImplicitNull = new DynamicJsNull(isExplicitNull: false);

        public static DynamicJsNull ExplicitNull = new DynamicJsNull(isExplicitNull: true);

        public readonly bool IsExplicitNull;

        private DynamicJsNull(bool isExplicitNull) : base(Types.Null)
        {
            IsExplicitNull = isExplicitNull;
        }

        public override object ToObject()
        {
            return null;
        }

        public override string ToString()
        {
            return "null";
        }

        public override bool Equals(JsValue other)
        {
            if (ReferenceEquals(this, other))
            {
                return true;
            }

            if (other is JsNull jsNull)
                return Equals(jsNull);

            if (other is DynamicJsNull dynamicJsNull)
                return Equals(dynamicJsNull);

            return false;
        }

        public bool Equals(JsNull other)
        {
            if (ReferenceEquals(null, other))
            {
                return false;
            }

            return true;
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
