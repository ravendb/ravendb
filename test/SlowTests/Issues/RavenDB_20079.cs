using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_20079 : RavenTestBase
{
    public RavenDB_20079(ITestOutputHelper output) : base(output)
    {
    }
    
    [Fact]
    public void CanCreateIndexWithDateTime()
    {
        using (var store = GetDocumentStore())
        {
            var index = new TestIndex();
            index.Execute(store);
        }
    }
    
    public class TestIndex : AbstractIndexCreationTask<Entity, TestIndex.Result>
    {
        public class Result
        {
            public DateTime DefaultDateTime { get; set; }
            public DateTime DateTimeMinValue { get; set; }
            public DateTime DateTime { get; set; }
            public string DateTimeString { get; set; }
        }
        public TestIndex()
        {
            Map = collection => from c in collection
                select new Result
                {
                    DefaultDateTime = default,
                    DateTimeMinValue = DateTime.MinValue,
                    DateTime = new DateTime(2023, 08, 16, 12,12,12),
                    DateTimeString = "2012-09-17T22:02:51.4021600"
                };
            
            StoreAllFields(FieldStorage.Yes);
        }
    }
}
