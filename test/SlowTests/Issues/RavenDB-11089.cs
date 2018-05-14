using System;
using System.Collections.Generic;
using System.Text;
using FastTests;
using Newtonsoft.Json.Serialization;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_11089 : RavenTestBase
    {
        public class Foo
        {
            public string Name;
            public Bar[] Bars;
        }

        public class Bar
        {
            public float Number;

        }

        [Fact]
        public void CustomSerializer()
        {
            using (var store = GetDocumentStore(options: new Options
            {
                ModifyDocumentStore = ss => ss.Conventions.CustomizeJsonSerializer = s =>
                {
                    s.ContractResolver = new CamelCasePropertyNamesContractResolver();
                }
            }))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Foo() { Name = "a", Bars = new Bar[] { new Bar() { Number = 1.0f }, new Bar() { Number = 2.0f } } });
                    session.Store(new Foo() { Name = "b", Bars = new Bar[] { new Bar() { Number = 3.0f } } });
                    session.SaveChanges();
                }


            }
        }
    }
}
