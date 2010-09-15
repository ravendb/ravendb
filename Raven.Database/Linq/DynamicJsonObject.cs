using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Dynamic;
using System.Linq;
using Newtonsoft.Json.Linq;

namespace Raven.Database.Linq
{
	/// <summary>
	/// A dynamic implementation on top of <see cref="JObject"/>
	/// </summary>
	public class DynamicJsonObject : DynamicObject
	{
		private readonly JObject obj;

		/// <summary>
		/// Gets the inner json object
		/// </summary>
		/// <value>The inner.</value>
	    public JObject Inner
	    {
	        get { return obj; }
	    }

		/// <summary>
		/// Initializes a new instance of the <see cref="DynamicJsonObject"/> class.
		/// </summary>
		/// <param name="obj">The obj.</param>
	    public DynamicJsonObject(JObject obj)
		{
			this.obj = obj;
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

		private static object TransformToValue(JToken jToken)
		{
			switch (jToken.Type)
			{
				case JTokenType.Object:
					var jObject = (JObject) jToken;
					var values = jObject.Value<JArray>("$values");
					if(values != null)
					{
						return new DynamicList(values.Select(TransformToValue).ToArray());
					}
					return new DynamicJsonObject(jObject);
				case JTokenType.Array:
					return new DynamicList(jToken.Select(TransformToValue).ToArray());
				default:
					var value = jToken.Value<object>();
					if(value is long)
					{
						var l = (long) value;
						if(l > int.MinValue && int.MaxValue > l)
							return (int) l;
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
			if (name == "__document_id")
			{
				return GetDocumentId();
			}
			JToken value;
			if (obj.TryGetValue(name, out value))
			{
				return TransformToValue(value);
			}
			return null;
		}

		/// <summary>
		/// Gets the document id.
		/// </summary>
		/// <returns></returns>
	    public string GetDocumentId()
		{
			var metadata = obj["@metadata"];
			if (metadata != null)
			{
				var id = metadata["@id"];
				if (id != null)
				{
					return id.Value<string>();
				}
			}
			return null;
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
				if(binder.Name == "DefaultIfEmpty")
				{
					if (inner.Length > 0)
						result = this;
					else
						result = new object[]{null};
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
				return ((IEnumerable<object>) inner).GetEnumerator();
			}

			IEnumerator IEnumerable.GetEnumerator()
			{
				return ((IEnumerable) inner).GetEnumerator();
			}

			/// <summary>
			/// Copies to the specified array
			/// </summary>
			/// <param name="array">The array.</param>
			/// <param name="index">The index.</param>
			public void CopyTo(Array array, int index)
			{
				((ICollection) inner).CopyTo(array, index);
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
		}
	}
}