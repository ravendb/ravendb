using Xunit;
using FastTests;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_16929 : RavenTestBase
    {
        public RavenDB_16929(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void DocumentWithStringWithNullCharacterAtEndShouldNotHaveChangeOnLoad()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var doc = new TestDoc {Id = "doc/1", DescriptionChar = 'a', Description = "TestString\0"};
                    session.Store(doc);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var doc = session.Load<TestDoc>("doc/1");
                    var t = doc.Description;
                    Assert.False(session.Advanced.HasChanges);
                    Assert.False(session.Advanced.HasChanged(doc));
                }
            }
        }

        [Fact]
        public void DocumentWithEmptyCharShouldNotHaveChangeOnLoad()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var doc = new TestDoc
                    {
                        Id = "doc/1",
                    };

                    session.Store(doc);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var doc = session.Load<TestDoc>("doc/1");
                    Assert.False(session.Advanced.HasChanges);
                    Assert.False(session.Advanced.HasChanged(doc));
                }
            }
        }

        public class TestDoc
        {
            public char DescriptionChar { get; set; }
            public string Id { get; set; }
            public string Description { get; set; }
        }
    }
}

