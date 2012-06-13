#if !NET35
using System;
using System.Collections;
using System.Collections.Generic;
using System.Dynamic;

namespace Raven.Abstractions.Linq
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

		public override bool TryInvokeMember(InvokeMemberBinder binder, object[] args, out object result)
		{
			switch (binder.Name)
			{
				case "Count":
					result = 0;
					return true;
				case "DefaultIfEmpty":
					result = new[]{this};
					return true;
				default:
					return base.TryInvokeMember(binder, args, out result);
			}
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

		// null is false or 0 by default
		public static implicit operator bool(DynamicNullObject o) { return false; }
		public static implicit operator bool?(DynamicNullObject o) { return null; }
		public static implicit operator decimal(DynamicNullObject o) { return 0; }
		public static implicit operator decimal?(DynamicNullObject o) { return null; }
		public static implicit operator double(DynamicNullObject o) { return 0; }
		public static implicit operator double?(DynamicNullObject o) { return null; }
		public static implicit operator float(DynamicNullObject o) { return 0; }
		public static implicit operator float?(DynamicNullObject o) { return null; }
		public static implicit operator long(DynamicNullObject o) { return 0; }
		public static implicit operator long?(DynamicNullObject o) { return null; }
		public static implicit operator string(DynamicNullObject o) { return null; }

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
			return right != null && (right is DynamicNullObject) == false;
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
#endif