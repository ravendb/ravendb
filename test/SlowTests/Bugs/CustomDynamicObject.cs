using System;
using System.Collections;
using System.Collections.Generic;
using System.Dynamic;
using FastTests;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Bugs
{
    public class CustomDynamicObject : RavenTestBase
    {
        public CustomDynamicObject(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanReadFromDB()
        {
            using(var s = GetDocumentStore())
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

        public class Customer : DynamicObject, IDictionary<string, object>
        {
            readonly Dictionary<string,object> inner = new Dictionary<string, object>();

            public override bool TryGetMember(GetMemberBinder binder, out object result)
            {
                object value;
                if(inner.TryGetValue(binder.Name,out value))
                {
                    result = value;
                    return true;
                }
                return base.TryGetMember(binder, out result);
            }

            public IEnumerator<KeyValuePair<string, object>> GetEnumerator()
            {
                return inner.GetEnumerator();
            }

            IEnumerator IEnumerable.GetEnumerator()
            {
                return GetEnumerator();
            }

            public void Add(KeyValuePair<string, object> item)
            {
                inner.Add(item.Key, item.Value);
            }

            public void Clear()
            {
                inner.Clear();
            }

            public bool Contains(KeyValuePair<string, object> item)
            {
                throw new NotImplementedException();
            }

            public void CopyTo(KeyValuePair<string, object>[] array, int arrayIndex)
            {
                throw new NotImplementedException();
            }

            public bool Remove(KeyValuePair<string, object> item)
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

            public void Add(string key, object value)
            {
                inner.Add(key,value);
            }

            public bool Remove(string key)
            {
                return inner.Remove(key);
            }

            public bool TryGetValue(string key, out object value)
            {
                return inner.TryGetValue(key, out value);
            }

            public object this[string key]
            {
                get { return inner[key]; }
                set { inner[key] = value; }
            }

            public ICollection<string> Keys
            {
                get { return inner.Keys; }
            }

            public ICollection<object> Values
            {
                get { return inner.Values; }
            }
        }
    }
}
