using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Tests.Common;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_4173 : RavenTest
    {
        public enum TestEnum
        {
            Blah,
        }

        [Fact]
        public void Fail()
        {
            using (var store = NewDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var item = new TestClass { Id = "Meep" };
                    session.Store(item);
                    session.SaveChanges();
                }

                var meep = store.DatabaseCommands.Get("Meep");
                var json = meep.ToJson();
                Console.WriteLine(json);

                Assert.Equal(json["DerivedEnums"].Values().First().ToString(), "Blah");
            }
        }

        public class TestClass
        {
            public string Id { get; set; }

            public IEnumerable<TestEnum> DerivedEnums
            {
                get
                {
                    var blah = new[] { TestEnum.Blah };
                    return blah.Union(blah);
                }
            }
        }
    }
}