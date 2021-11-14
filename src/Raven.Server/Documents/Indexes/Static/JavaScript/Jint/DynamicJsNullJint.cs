using System;
using Jint.Native;
using Jint.Runtime;

namespace Raven.Server.Documents.Indexes.Static.JavaScript.Jint
{
    public sealed class DynamicJsNullJint : JsValue, IEquatable<JsNull>, IEquatable<DynamicJsNullJint>
    {
        public static DynamicJsNullJint ImplicitNullJint = new DynamicJsNullJint(isExplicitNull: false);

        public static DynamicJsNullJint ExplicitNullJint = new DynamicJsNullJint(isExplicitNull: true);

        public readonly bool IsExplicitNull;

        private DynamicJsNullJint(bool isExplicitNull) : base(Types.Null)
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

            if (other is DynamicJsNullJint dynamicJsNull)
                return Equals(dynamicJsNull);

            return false;
        }

        public bool Equals(JsNull other)
        {
            return true;
        }

        public bool Equals(DynamicJsNullJint other)
        {
            return true;
        }

        public override bool Equals(object other)
        {
            if (other == null)
                return true;

            if (other is JsValue jsValue)
                return Equals(jsValue);

            return false;
        }

        public override int GetHashCode()
        {
            return Null.GetHashCode();
        }
    }
}
