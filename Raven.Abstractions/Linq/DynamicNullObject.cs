using System;
using System.Collections;
using System.Collections.Generic;
using System.Dynamic;

namespace Raven.Database.Linq
{
    public class DynamicNullObject : DynamicObject, IEnumerable<object>
    {
        public override string ToString()
        {
            return String.Empty;
        }

        public bool IsExplicitNull { get; set; }

        public override bool TryGetIndex(GetIndexBinder binder, object[] indexes, out object result)
        {
            result = this;
            return true;
        }

        public override bool TryGetMember(GetMemberBinder binder, out object result)
        {
            result = this;
            return true;
        }

        public IEnumerator<object> GetEnumerator()
        {
            yield break;
        }

        public override bool TryInvoke(InvokeBinder binder, object[] args, out object result)
        {
            result = this;
            return true;
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        // null is false by default
        public static implicit operator bool (DynamicNullObject o)
        {
            return false;
        }

        public override bool Equals(object obj)
        {
            return obj is DynamicNullObject;
        }

        public override int GetHashCode()
        {
            return 0;
        }

        public static bool operator ==(DynamicNullObject left, object right)
        {
            return right == null || right is DynamicNullObject;
        }

        public static bool operator !=(DynamicNullObject left, object right)
        {
            return right != null && (right is DynamicNullObject) == false ;
        }

        public static bool operator >(DynamicNullObject left, object right)
        {
            return false;
        }

        public static bool operator <(DynamicNullObject left, object right)
        {
            return false;
        }

        public static bool operator >=(DynamicNullObject left, object right)
        {
            return false;
        }

        public static bool operator <=(DynamicNullObject left, object right)
        {
            return false;
        }
    }
}