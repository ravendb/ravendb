using System;
using System.Collections;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq.Expressions;
using Raven.Imports.Newtonsoft.Json.Utilities;

namespace Raven.Abstractions.Linq
{
    public class DynamicNullObject : DynamicObject, IEnumerable<object>, IComparable
    {
        public override string ToString()
        {
            return String.Empty;
        }

        public bool IsExplicitNull { get; set; }

        public override bool TryConvert(ConvertBinder binder, out object result)
        {
            result = binder.ReturnType.IsValueType()
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
                    result = new DynamicNullObject();
                    break;
            }
            return true;
        }

        public override bool TryGetIndex(GetIndexBinder binder, object[] indexes, out object result)
        {
            result = new DynamicNullObject
            {
                IsExplicitNull = false
            };
            return true;
        }

        public override bool TryGetMember(GetMemberBinder binder, out object result)
        {
            if (binder.Name == "HasValue")
            {
                result = false;
                return true;
            }
            result = new DynamicNullObject
            {
                IsExplicitNull = false
            };
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
                    result = new DynamicNullObject {IsExplicitNull = true};
                    return true;
                case "Count":
                    result = 0;
                    return true;
                case "DefaultIfEmpty":
                    result = new[]
                    {
                        new DynamicNullObject
                        {
                            IsExplicitNull = false
                        }
                    };
                    return true;
                default:
                    return base.TryInvokeMember(binder, args, out result);
            }
        }

        public override bool TryInvoke(InvokeBinder binder, object[] args, out object result)
        {
            result = new DynamicNullObject
            {
                IsExplicitNull = false
            };
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

        public static implicit operator double(DynamicNullObject o) { return double.NaN; }
        public static implicit operator double?(DynamicNullObject o) { return null; }

        public static implicit operator int(DynamicNullObject o) { return 0; }
        public static implicit operator int?(DynamicNullObject o) { return null; }

        public static implicit operator long(DynamicNullObject o) { return 0; }
        public static implicit operator long?(DynamicNullObject o) { return null; }

        public static implicit operator decimal(DynamicNullObject o) { return 0; }
        public static implicit operator decimal?(DynamicNullObject o) { return null; }

        public static implicit operator float(DynamicNullObject o) { return float.NaN; }
        public static implicit operator float?(DynamicNullObject o) { return null; }
    
        public override bool Equals(object obj)
        {
            return obj is DynamicNullObject;
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

        public static implicit operator string(DynamicNullObject self)
        {
            return null;
        }
    }
}
