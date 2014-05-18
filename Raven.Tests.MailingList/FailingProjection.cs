using System.Linq;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.MailingList
{
    public class FailingProjection : RavenTest
    {
        public class MyClass
        {
            public string Prop1 { get; set; }
            public string Prop2 { get; set; }
            public int Index { get; set; }
        }

        [Fact]
        public void TestFailingProjection()
        {
           using (var store = NewRemoteDocumentStore())
           {
               using (var session = store.OpenSession())
               {
                   session.Store(new MyClass { Index = 1, Prop1 = "prop1", Prop2 = "prop2" });
                   session.Store(new MyClass { Index = 2, Prop1 = "prop1", Prop2 = "prop2" });
                   session.Store(new MyClass { Index = 3, Prop1 = "prop1", Prop2 = "prop2" });

                   session.SaveChanges();
               }
               using (var session = store.OpenSession())
               {
                   store.DatabaseCommands
                       .PutIndex("MyClass/ByIndex",
                       new IndexDefinitionBuilder<MyClass>
                       {
                           Map = docs => from doc in docs select new { Index = doc.Index }
                       }, true);

                   WaitForIndexing(store);

                   var query = session.Query<MyClass>("MyClass/ByIndex")
                   .Select(x => new MyClass
                   {
                       Index = x.Index,
                       Prop1 = x.Prop1
                   });

                   var enumerator = session.Advanced.Stream(query);
                   int count = 0;
                   while (enumerator.MoveNext())
                   {
                       Assert.IsType<MyClass>(enumerator.Current.Document);
                       Assert.Null(enumerator.Current.Document.Prop2);
                       count++;
                   }

                   Assert.Equal(3, count);
               }
           }
        }
    }
}

