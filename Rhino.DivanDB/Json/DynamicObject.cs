using System;

namespace Rhino.DivanDB.Json
{
    public abstract class DynamicObject
    {
        public abstract DynamicObject this[string key] { get; }

        protected abstract object Value { get; }

        public string Unwrap()
        {
            return Value.ToString();
        }

        public static bool operator==(DynamicObject dyn, string str)
        {
            if (ReferenceEquals(dyn, null))
                return str == null;
            return dyn.Value is string && dyn.Value.Equals(str);
        }

        public static implicit operator int(DynamicObject dyn)
        {
            return (int) (long) dyn;
        }

        public static implicit operator long(DynamicObject dyn)
        {
            if (dyn.Value is long)
                return (long)dyn.Value;
            throw new InvalidCastException("Cannot convert value to integer because it is: " + (dyn.Value ?? "null"));
        }

        public static bool operator !=(DynamicObject dyn, string str)
        {
            return Equals(dyn.Value, str) == false;
        }

        public bool Equals(DynamicObject other)
        {
            return !ReferenceEquals(null, other);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != typeof (DynamicObject)) return false;
            return Equals((DynamicObject) obj);
        }

        public override int GetHashCode()
        {
            return 0;
        }
    }
}