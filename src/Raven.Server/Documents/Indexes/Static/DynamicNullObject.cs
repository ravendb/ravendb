using System;
using System.Collections;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq.Expressions;
using System.Reflection;
using Sparrow.Json;

namespace Raven.Server.Documents.Indexes.Static
{
    public class DynamicNullObject : DynamicObject, IEnumerable<object>, IComparable
    {
        public static DynamicNullObject Null = new DynamicNullObject(isExplicitNull: false);

        public static DynamicNullObject ExplicitNull = new DynamicNullObject(isExplicitNull: true);

        private DynamicNullObject(bool isExplicitNull)
        {
            IsExplicitNull = isExplicitNull;
        }

        public override string ToString()
        {
            return string.Empty;
        }

        public readonly bool IsExplicitNull;

        public override bool TryConvert(ConvertBinder binder, out object result)
        {
            result = binder.ReturnType.IsValueType
                         ? Activator.CreateInstance(binder.ReturnType)
                         : null;
            return true;
        }

        public override bool TryBinaryOperation(BinaryOperationBinder binder, object arg, out object result)
        {
            switch (binder.Operation)
            {
                case ExpressionType.Equal:
                    result = arg == null || arg is DynamicNullObject;
                    break;
                case ExpressionType.NotEqual:
                    result = arg != null && arg is DynamicNullObject == false;
                    break;
                default:
                    result = Null;
                    break;
            }
            return true;
        }

        public override bool TryGetIndex(GetIndexBinder binder, object[] indexes, out object result)
        {
            result = Null;
            return true;
        }

        public override bool TryGetMember(GetMemberBinder binder, out object result)
        {
            if (binder.Name == "HasValue")
            {
                result = false;
                return true;
            }
            result = Null;
            return true;
        }

        public IEnumerator<object> GetEnumerator()
        {
            yield break;
        }

        public override bool TryInvokeMember(InvokeMemberBinder binder, object[] args, out object result)
        {
            switch (binder.Name)
            {
                case "GetValueOrDefault":
                    result = ExplicitNull;
                    return true;
                case "Count":
                    result = 0;
                    return true;
                case "DefaultIfEmpty":
                    result = new[]
                    {
                        Null
                    };
                    return true;
                default:
                    return base.TryInvokeMember(binder, args, out result);
            }
        }

        public override bool TryInvoke(InvokeBinder binder, object[] args, out object result)
        {
            result = Null;
            return true;
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public static bool operator true(DynamicNullObject left)
        {
            return false;
        }

        public static bool operator false(DynamicNullObject left)
        {
            return false;
        }

        public static dynamic operator ~(DynamicNullObject left)
        {
            return left;
        }

        public static dynamic operator !(DynamicNullObject left)
        {
            return left;
        }

        public static dynamic operator ^(DynamicNullObject left, object right)
        {
            return left;
        }

        public static dynamic operator &(DynamicNullObject left, object right)
        {
            return left;
        }

        public static dynamic operator |(DynamicNullObject left, object right)
        {
            return left;
        }

        public static DynamicNullObject operator ++(DynamicNullObject left)
        {
            return left;
        }

        public static DynamicNullObject operator --(DynamicNullObject left)
        {
            return left;
        }

        public static dynamic operator +(DynamicNullObject left, object right)
        {
            if (right is string || right is LazyStringValue || right is LazyCompressedStringValue) 
                return right; // match string concat on null
            return left;
        }

        public static dynamic operator -(DynamicNullObject left, object right)
        {
            return left;
        }

        public static dynamic operator *(DynamicNullObject left, object right)
        {
            return left;
        }

        public static dynamic operator /(DynamicNullObject left, object right)
        {
            return left;
        }

        public static dynamic operator %(DynamicNullObject left, object right)
        {
            return left;
        }

        public static bool operator >=(DynamicNullObject left, object right)
        {
            return false;
        }

        public static bool operator <=(DynamicNullObject left, object right)
        {
            return false;
        }

        public static bool operator >(DynamicNullObject left, object right)
        {
            return false;
        }

        public static bool operator <(DynamicNullObject left, object right)
        {
            return false;
        }

        public static bool operator ==(DynamicNullObject left, object right)
        {
            return right == null || right is DynamicNullObject;
        }

        public static bool operator !=(DynamicNullObject left, object right)
        {
            return right != null && (right is DynamicNullObject) == false;
        }

        public static implicit operator double(DynamicNullObject o)
        {
            return double.NaN;
        }

        public static implicit operator double?(DynamicNullObject o)
        {
            return null;
        }

        public static implicit operator int(DynamicNullObject o)
        {
            return 0;
        }

        public static implicit operator int?(DynamicNullObject o)
        {
            return null;
        }

        public static implicit operator long(DynamicNullObject o)
        {
            return 0;
        }

        public static implicit operator long?(DynamicNullObject o)
        {
            return null;
        }

        public static implicit operator decimal(DynamicNullObject o)
        {
            return 0;
        }

        public static implicit operator decimal?(DynamicNullObject o)
        {
            return null;
        }

        public static implicit operator float(DynamicNullObject o)
        {
            return float.NaN;
        }

        public static implicit operator float?(DynamicNullObject o)
        {
            return null;
        }

        public static implicit operator string(DynamicNullObject self)
        {
            return null;
        }

        public override bool Equals(object obj)
        {
            return obj == null || obj is DynamicNullObject;
        }

        public override int GetHashCode()
        {
            return 0;
        }

        public int CompareTo(object obj)
        {
            if (obj is DynamicNullObject || obj == null)
                return 0;
            return -1;
        }

        public dynamic this[string key] => this;

        public dynamic this[long key] => this;

        public dynamic this[double key] => this;
    }
}
