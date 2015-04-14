using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Raven.Tests.Common;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_3248 : RavenTest
    {
        [Fact]
        public void StreamQueryShouldWorkEvenIfWaitForNoneStaleResualtIsSet()
        {
            using (var store = NewDocumentStore())
            {
                store.Conventions.DefaultQueryingConsistency = ConsistencyOptions.AlwaysWaitForNonStaleResultsAsOfLastWrite;
                new RavenDB_3248_TestObject_ByName().Execute(store);
                using (var session = store.OpenSession())
                {
                    for (int i = 0; i < 20; i++)
                    {
                        session.Store(new RavenDB_3248_TestObject());
                    }
                    session.SaveChanges();
                }

                WaitForIndexing(store);
                int count = 0;
                using (var session = store.OpenSession())
                {
                    var query = session.Query<RavenDB_3248_TestObject, RavenDB_3248_TestObject_ByName>();

                    var reader = session.Advanced.Stream(query);

                    while (reader.MoveNext())
                    {
                        count++;
                        Assert.IsType<RavenDB_3248_TestObject>(reader.Current.Document);
                    }
                }
                Assert.Equal(count,20);
            }
            
        }
    }
    public class RavenDB_3248_TestObject_ByName : AbstractIndexCreationTask<RavenDB_3248_TestObject>
    {
        public RavenDB_3248_TestObject_ByName()
        {
            Map = users => from u in users select new { u.Name };
        }
    }
    public class RavenDB_3248_TestObject
    {
        public string Name { get; set; }
    }
}
