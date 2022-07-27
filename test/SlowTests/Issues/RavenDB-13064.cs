using System;
using FastTests;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_13064 : RavenTestBase
    {
        public RavenDB_13064(ITestOutputHelper output) : base(output)
        {
        }

        [Theory]
        [RavenData(JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public void CanPatchNestedSubclass(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var cat = new Pet { Id = Guid.NewGuid().ToString(), Name = "Cat" };
                var john = new Person { Id = Guid.NewGuid().ToString(), Name = "John", Pet = cat };

                using (var session = store.OpenSession())
                {
                    session.Store(john);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var load = session.Load<Person>(john.Id);
                }

                using (var session = store.OpenSession())
                {
                    var dog = new Pet { Id = Guid.NewGuid().ToString(), Name = "Dog" };
                    session.Advanced.Patch<Person, IPet>(john.Id, x => x.Pet, dog);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    // should not throw
                    var load = session.Load<Person>(john.Id);
                }
            }
        }

        private class Person
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public IPet Pet { get; set; }
        }

        private interface IPet
        {

        }

        private class Pet : IPet
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }

    }
}
