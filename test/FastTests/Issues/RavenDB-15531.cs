using Xunit;
using Xunit.Abstractions;
using System.Linq;
using Raven.Client.Documents.Session;


namespace FastTests.Issues
{
    public class RavenDB_15531 : RavenTestBase
    {
        public RavenDB_15531(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void ShouldWork()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    SimpleDoc doc = new SimpleDoc() { Id = "TestDoc", Name = "State1" };
                    session.Store(doc);
                    session.SaveChanges();

                    doc.Name = "State2";
                    var changes1 = session.Advanced.WhatChanged();
                    Assert.Equal(DocumentsChanges.ChangeType.FieldChanged, changes1.First().Value.First().Change);
                    Assert.Equal(nameof(SimpleDoc.Name), changes1.First().Value.First().FieldName);
                    Assert.Equal("State1", changes1.First().Value.First().FieldOldValue.ToString());
                    Assert.Equal("State2", changes1.First().Value.First().FieldNewValue.ToString());

                    session.SaveChanges();

                    doc.Name = "State3";
                    var changes2 = session.Advanced.WhatChanged();
                    Assert.Equal(DocumentsChanges.ChangeType.FieldChanged, changes2.First().Value.First().Change);
                    Assert.Equal(nameof(SimpleDoc.Name), changes2.First().Value.First().FieldName);
                    Assert.Equal("State2", changes2.First().Value.First().FieldOldValue.ToString());
                    Assert.Equal("State3", changes2.First().Value.First().FieldNewValue.ToString());

                    session.Advanced.Refresh(doc);
                    doc.Name = "State4";
                    var changes3 = session.Advanced.WhatChanged();
                    Assert.NotEmpty(changes3);
                    Assert.Equal(DocumentsChanges.ChangeType.FieldChanged, changes3.First().Value.First().Change);
                    Assert.Equal(nameof(SimpleDoc.Name), changes3.First().Value.First().FieldName);
                    Assert.Equal("State2", changes3.First().Value.First().FieldOldValue.ToString());
                    Assert.Equal("State4", changes3.First().Value.First().FieldNewValue.ToString());
                }
            }
        }
        public class SimpleDoc
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }
    }
}
