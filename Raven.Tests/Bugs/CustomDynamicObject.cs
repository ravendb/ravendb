using System;
using System.Collections;
using System.Collections.Generic;
using System.Dynamic;
using System.Runtime.Serialization;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class CustomDynamicObject : RavenTest
	{
		[Fact]
		public void CanReadFromDB()
		{
			using(var s = NewDocumentStore())
			{
				using(var session = s.OpenSession())
				{
					session.Store(new Customer
					{
						{"Id", "customers/1"},
						{"Ayende", "Rahien"}
					});
					session.SaveChanges();
				}
				using (var session = s.OpenSession())
				{
					dynamic load = session.Load<Customer>("customers/1");
					Assert.Equal("Rahien", load.Ayende);
				}

			}
		}

		public class Customer : DynamicObject, IDictionary<string,string>
		{
			readonly Dictionary<string,string> inner = new Dictionary<string, string>();

			public override bool TryGetMember(GetMemberBinder binder, out object result)
			{
				string value;
				if(inner.TryGetValue(binder.Name,out value))
				{
					result = value;
					return true;
				}
				return base.TryGetMember(binder, out result);
			}

			public IEnumerator<KeyValuePair<string, string>> GetEnumerator()
			{
				return inner.GetEnumerator();
			}

			IEnumerator IEnumerable.GetEnumerator()
			{
				return GetEnumerator();
			}

			public void Add(KeyValuePair<string, string> item)
			{
				inner.Add(item.Key, item.Value);
			}

			public void Clear()
			{
				inner.Clear();
			}

			public bool Contains(KeyValuePair<string, string> item)
			{
				throw new NotImplementedException();
			}

			public void CopyTo(KeyValuePair<string, string>[] array, int arrayIndex)
			{
				throw new NotImplementedException();
			}

			public bool Remove(KeyValuePair<string, string> item)
			{
				throw new NotImplementedException();
			}

			public int Count
			{
				get { return inner.Count; }
			}

			public bool IsReadOnly
			{
				get { return false; }
			}

			public bool ContainsKey(string key)
			{
				return inner.ContainsKey(key);
			}

			public void Add(string key, string value)
			{
				inner.Add(key,value);
			}

			public bool Remove(string key)
			{
				return inner.Remove(key);
			}

			public bool TryGetValue(string key, out string value)
			{
				return inner.TryGetValue(key, out value);
			}

			public string this[string key]
			{
				get { return inner[key]; }
				set { inner[key] = value; }
			}

			public ICollection<string> Keys
			{
				get { return inner.Keys; }
			}

			public ICollection<string> Values
			{
				get { return inner.Values; }
			}
		}
	}
}