//-----------------------------------------------------------------------
// <copyright file="DynamicJsonObject.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
#if !NET_3_5
using System;
using System.Collections;
using System.Collections.Generic;
using System.Dynamic;
using System.Globalization;
using System.Linq;
using Newtonsoft.Json.Linq;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Json.Linq;

namespace Raven.Database.Linq
{
	/// <summary>
	/// A dynamic implementation on top of <see cref="RavenJObject"/>
	/// </summary>
	public class DynamicJsonObject : DynamicObject, IEnumerable<object>
	{
		public IEnumerator<dynamic> GetEnumerator()
		{
			foreach (var item in Inner)
			{
                if(item.Key[0] == '$')
                    continue;

				yield return new KeyValuePair<string,object>(item.Key, TransformToValue(item.Value));
			}
		}

		/// <summary>
		/// Returns a <see cref="T:System.String"/> that represents the current <see cref="T:System.Object"/>.
		/// </summary>
		/// <returns>
		/// A <see cref="T:System.String"/> that represents the current <see cref="T:System.Object"/>.
		/// </returns>
		/// <filterpriority>2</filterpriority>
		public override string ToString()
		{
			return inner.ToString();
		}

		/// <summary>
		/// Determines whether the specified <see cref="T:System.Object"/> is equal to the current <see cref="T:System.Object"/>.
		/// </summary>
		/// <returns>
		/// true if the specified <see cref="T:System.Object"/> is equal to the current <see cref="T:System.Object"/>; otherwise, false.
		/// </returns>
		/// <param name="other">The <see cref="T:System.Object"/> to compare with the current <see cref="T:System.Object"/>. </param><filterpriority>2</filterpriority>
		public override bool Equals(object other)
		{
			var dynamicJsonObject = other as DynamicJsonObject;
			if (dynamicJsonObject != null)
				return new RavenJTokenEqualityComparer().Equals(inner, dynamicJsonObject.inner);
			return base.Equals(other);
		}

		/// <summary>
		/// Serves as a hash function for a particular type. 
		/// </summary>
		/// <returns>
		/// A hash code for the current <see cref="T:System.Object"/>.
		/// </returns>
		/// <filterpriority>2</filterpriority>
		public override int GetHashCode()
		{
			return new RavenJTokenEqualityComparer().GetHashCode(inner);
		}

		IEnumerator IEnumerable.GetEnumerator()
		{
			return GetEnumerator();
		}

		private readonly RavenJObject inner;

		/// <summary>
		/// Gets the inner json object
		/// </summary>
		/// <value>The inner.</value>
		public RavenJObject Inner
		{
			get { return inner; }
		}

		/// <summary>
		/// Initializes a new instance of the <see cref="DynamicJsonObject"/> class.
		/// </summary>
		/// <param name="inner">The obj.</param>
		public DynamicJsonObject(RavenJObject inner)
		{
			this.inner = inner;
		}

		/// <summary>
		/// Provides the implementation for operations that get member values. Classes derived from the <see cref="T:System.Dynamic.DynamicObject"/> class can override this method to specify dynamic behavior for operations such as getting a value for a property.
		/// </summary>
		/// <param name="binder">Provides information about the object that called the dynamic operation. The binder.Name property provides the name of the member on which the dynamic operation is performed. For example, for the Console.WriteLine(sampleObject.SampleProperty) statement, where sampleObject is an instance of the class derived from the <see cref="T:System.Dynamic.DynamicObject"/> class, binder.Name returns "SampleProperty". The binder.IgnoreCase property specifies whether the member name is case-sensitive.</param>
		/// <param name="result">The result of the get operation. For example, if the method is called for a property, you can assign the property value to <paramref name="result"/>.</param>
		/// <returns>
		/// true if the operation is successful; otherwise, false. If this method returns false, the run-time binder of the language determines the behavior. (In most cases, a run-time exception is thrown.)
		/// </returns>
		public override bool TryGetMember(GetMemberBinder binder, out object result)
		{
			result = GetValue(binder.Name);
			return true;
		}

		/// <summary>
		/// Provides the implementation for operations that get a value by index. Classes derived from the <see cref="T:System.Dynamic.DynamicObject"/> class can override this method to specify dynamic behavior for indexing operations.
		/// </summary>
		/// <returns>
		/// true if the operation is successful; otherwise, false. If this method returns false, the run-time binder of the language determines the behavior. (In most cases, a run-time exception is thrown.)
		/// </returns>
		public override bool TryGetIndex(GetIndexBinder binder, object[] indexes, out object result)
		{
			if (indexes.Length != 1 || indexes[0] is string == false)
			{
				result = null;
				return false;
			}
			result = GetValue((string)indexes[0]);
			return true;
		}

	    public static object TransformToValue(RavenJToken jToken)
		{
			switch (jToken.Type)
			{
				case JTokenType.Object:
					var jObject = (RavenJObject)jToken;
					var values = jObject.Value<RavenJArray>("$values");
					if (values != null)
					{
						return new DynamicList(values.Select(TransformToValue).ToArray());
					}
					return new DynamicJsonObject(jObject);
				case JTokenType.Array:
					var ar = jToken as RavenJArray; // cannot result in null because jToken.Type is set to Array
					return new DynamicList(ar.Select(TransformToValue).ToArray());
				case JTokenType.Date:
					return jToken.Value<DateTime>();
				case JTokenType.Null:
					return new DynamicNullObject{IsExplicitNull = true};
				default:
					var value = jToken.Value<object>();
					if (value is long)
					{
						var l = (long)value;
						if (l > int.MinValue && int.MaxValue > l)
							return (int)l;
					}
					var s = value as string;
					if(s != null)
					{
						DateTime time;
                        if (DateTime.TryParseExact(s, Default.DateTimeFormatsToRead, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out time))
							return time;
					}
					return value;
			}
		}

		/// <summary>
		/// Gets the value for the specified name
		/// </summary>
		/// <param name="name">The name.</param>
		/// <returns></returns>
		public object GetValue(string name)
		{
			if (name == Constants.DocumentIdFieldName)
			{
				return GetDocumentId();
			}
			RavenJToken value;
			if (inner.TryGetValue(name, out value))
			{
				return TransformToValue(value);
			}
            if(name.StartsWith("_"))
            {
                if (inner.TryGetValue(name.Substring(1), out value))
                {
                    return TransformToValue(value);
                } 
            }
			if(name == "Id")
			{
				return GetDocumentId();
			}
			if(name == "Inner")
			{
				return Inner;
			}
			return new DynamicNullObject();
		}

		/// <summary>
		/// Gets the document id.
		/// </summary>
		/// <returns></returns>
		public object GetDocumentId()
		{
			var metadata = inner["@metadata"] as RavenJObject;
			if (metadata != null)
			{
				var id = metadata.Value<string>("@id");
				return string.IsNullOrEmpty(id) ? (object)new DynamicNullObject() : id;
			}
			return inner.Value<string>(Constants.DocumentIdFieldName) ?? (object)new DynamicNullObject();
		}

		/// <summary>
		/// A list that responds to the dynamic object protocol
		/// </summary>
		public class DynamicList : DynamicObject, IEnumerable<object>
		{
			private readonly object[] inner;

			/// <summary>
			/// Initializes a new instance of the <see cref="DynamicList"/> class.
			/// </summary>
			/// <param name="inner">The inner.</param>
			public DynamicList(object[] inner)
			{
				this.inner = inner;
			}

			/// <summary>
			/// Provides the implementation for operations that invoke a member. Classes derived from the <see cref="T:System.Dynamic.DynamicObject"/> class can override this method to specify dynamic behavior for operations such as calling a method.
			/// </summary>
			/// <returns>
			/// true if the operation is successful; otherwise, false. If this method returns false, the run-time binder of the language determines the behavior. (In most cases, a language-specific run-time exception is thrown.)
			/// </returns>
			public override bool TryInvokeMember(InvokeMemberBinder binder, object[] args, out object result)
			{
				switch (binder.Name)
				{
					case "Count":
						if (args.Length == 0)
						{
							result = Count;
							return true;
						}
						result = Enumerable.Count<dynamic>(this, (Func<dynamic, bool>)args[0]);
						return true;
					case "DefaultIfEmpty":
						if (inner.Length > 0)
							result = this;
						else
							result = new object[] { null };
						return true;
				}
				return base.TryInvokeMember(binder, args, out result);
			}

			/// <summary>
			/// Gets the enumerator.
			/// </summary>
			/// <returns></returns>
			public IEnumerator<object> GetEnumerator()
			{
				return ((IEnumerable<object>)inner).GetEnumerator();
			}

			IEnumerator IEnumerable.GetEnumerator()
			{
				return ((IEnumerable)inner).GetEnumerator();
			}

			/// <summary>
			/// Copies to the specified array
			/// </summary>
			/// <param name="array">The array.</param>
			/// <param name="index">The index.</param>
			public void CopyTo(Array array, int index)
			{
				((ICollection)inner).CopyTo(array, index);
			}

			/// <summary>
			/// Gets the sync root.
			/// </summary>
			/// <value>The sync root.</value>
			public object SyncRoot
			{
				get { return inner.SyncRoot; }
			}

			/// <summary>
			/// Gets a value indicating whether this instance is synchronized.
			/// </summary>
			/// <value>
			/// 	<c>true</c> if this instance is synchronized; otherwise, <c>false</c>.
			/// </value>
			public bool IsSynchronized
			{
				get { return inner.IsSynchronized; }
			}

			/// <summary>
			/// Gets or sets the <see cref="System.Object"/> at the specified index.
			/// </summary>
			/// <value></value>
			public object this[int index]
			{
				get { return inner[index]; }
				set { inner[index] = value; }
			}

			/// <summary>
			/// Gets a value indicating whether this instance is fixed size.
			/// </summary>
			/// <value>
			/// 	<c>true</c> if this instance is fixed size; otherwise, <c>false</c>.
			/// </value>
			public bool IsFixedSize
			{
				get { return inner.IsFixedSize; }
			}

			/// <summary>
			/// Determines whether the list contains the specified item.
			/// </summary>
			/// <param name="item">The item.</param>
			public bool Contains(object item)
			{
				return inner.Contains(item);
			}

			/// <summary>
			/// Find the index of the specified item in the list
			/// </summary>
			/// <param name="item">The item.</param>
			/// <returns></returns>
			public int IndexOf(object item)
			{
				return Array.IndexOf(inner, item);
			}

			/// <summary>
			/// Find the index of the specified item in the list
			///  </summary>
			/// <param name="item">The item.</param>
			/// <param name="index">The index.</param>
			/// <returns></returns>
			public int IndexOf(object item, int index)
			{
				return Array.IndexOf(inner, item, index);
			}

			/// <summary>
			/// Find the index of the specified item in the list/// 
			/// </summary>
			/// <param name="item">The item.</param>
			/// <param name="index">The index.</param>
			/// <param name="count">The count.</param>
			/// <returns></returns>
			public int IndexOf(object item, int index, int count)
			{
				return Array.IndexOf(inner, item, index, count);
			}

			/// <summary>
			/// Gets the count.
			/// </summary>
			/// <value>The count.</value>
			public int Count
			{
				get { return inner.Length; }
			}

			/// <summary>
			/// Gets the length.
			/// </summary>
			/// <value>The length.</value>
			public int Length
			{
				get { return inner.Length; }
			}

			public IEnumerable<dynamic> Select(Func<dynamic,dynamic > func)
			{
				return inner.Select(item => func(item));
			}
		}
	}

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
#endif