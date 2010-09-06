using System;
using System.IO;
using System.Linq;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Xunit;

namespace Raven.Tests.Bugs
{
    public class HiLoKeyIssues : IDisposable
    {
        private DocumentStore documentStore;

        private void CreateFreshDocumentStore() {
            if (documentStore != null)
                documentStore.Dispose();

            if (Directory.Exists("HiLoData")) Directory.Delete("HiLoData", true);
            documentStore = new DocumentStore();
            documentStore.Configuration.DataDirectory = "HiLoData";
            documentStore.Initialize();

            documentStore.DatabaseCommands.PutIndex("Foo/Something", new IndexDefinition<Foo> {
                                                                                                  Map = docs => from doc in docs select new { doc.Something }
                                                                                              });
        }

        [Fact]
        public void Generated_HiLoKey_Is_Duplicate_Of_Manually_Set_Key()
        {
            CreateFreshDocumentStore();

            string firstId;
            using (var session = documentStore.OpenSession()) {
                var foo = new Foo() { Id = "foos/1", Something = "something1" };
                session.Store(foo);
                Assert.Equal("foos/1", foo.Id);
                firstId = foo.Id;
                session.SaveChanges();
            }

            string secondId;
            using (var session = documentStore.OpenSession()) {
                var foo = new Foo() { Something = "something2" };
                Assert.Null(foo.Id);
                session.Store(foo);
                Assert.NotNull(foo.Id);
                Console.WriteLine("Second id = " + foo.Id);
                Assert.NotEqual(firstId, foo.Id);
                secondId = foo.Id;
                session.SaveChanges();
            }
        }

        [Fact]
        public void Document_With_Generated_HiLoKey_Overwrites_Document_With_Manual_Key()
        {
            CreateFreshDocumentStore();

            string firstId = "foos/1";
            using (var session = documentStore.OpenSession()) {
                var foo = new Foo() { Id = firstId, Something = "something1" };
                session.Store(foo);
                firstId = foo.Id;
                session.SaveChanges();
            }

            string secondId;
            using (var session = documentStore.OpenSession()) {
                var foo = new Foo() { Something = "something2" };
                session.Store(foo);
                Console.WriteLine("Second id = " + foo.Id);
                secondId = foo.Id;
                session.SaveChanges();
            }

            using (var session = documentStore.OpenSession()) {
                var foo1 = session.LuceneQuery<Foo>("Foo/Something").WaitForNonStaleResults().FirstOrDefault(x => x.Id == firstId);
                var foo2 = session.LuceneQuery<Foo>("Foo/Something").WaitForNonStaleResults().FirstOrDefault(x => x.Id == secondId);
                var count = session.LuceneQuery<Foo>("Foo/Something").WaitForNonStaleResults().Count();

                Console.WriteLine("count = " + count);
                Console.WriteLine("foo1 .id= " + foo1.Id + " .something = " + foo1.Something);
                Console.WriteLine("foo2 .id= " + foo2.Id + " .something = " + foo2.Something);
                Assert.Equal("something2", foo2.Something);
                Assert.Equal("something1", foo2.Something);
                Assert.Equal(2, count);
            }

        }

        public class Foo
        {
            public string Id { get; set; }
            public string Something { get; set; }
        }

        public void Dispose()
        {
            if (documentStore != null)
                documentStore.Dispose();
            if (Directory.Exists("HiLoData")) Directory.Delete("HiLoData", true);
        }
    }
}
