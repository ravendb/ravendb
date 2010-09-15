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

		public override bool TryGetMember(GetMemberBinder binder, out object result)
		{
			result = GetValue(binder.Name);
			return true;
		}

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

			public IEnumerator<object> GetEnumerator()
			{
				return ((IEnumerable<object>) inner).GetEnumerator();
			}

			IEnumerator IEnumerable.GetEnumerator()
			{
				return ((IEnumerable) inner).GetEnumerator();
			}

			public void CopyTo(Array array, int index)
			{
				((ICollection) this.inner).CopyTo(array, index);
			}

			public object SyncRoot
			{
				get { return inner.SyncRoot; }
			}

			public bool IsSynchronized
			{
				get { return inner.IsSynchronized; }
			}

			public object this[int index]
			{
				get { return inner[index]; }
				set { inner[index] = value; }
			}

			public bool IsFixedSize
			{
				get { return inner.IsFixedSize; }
			}

			public bool Contains(object item)
			{
				return inner.Contains(item);
			}

			public int IndexOf(object item)
			{
				return Array.IndexOf(inner, item);
			}

			public int IndexOf(object item, int index)
			{
				return Array.IndexOf(inner, item, index);
			}

			public int IndexOf(object item, int index, int count)
			{
				return Array.IndexOf(inner, item, index, count);
			}

			public int Count
			{
				get { return inner.Length; }
			}

			public int Length
			{
				get { return inner.Length; }
			}
		}
	}
}