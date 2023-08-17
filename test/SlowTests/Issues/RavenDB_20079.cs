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
            public TimeOnly DefaultTimeOnly { get; set; }
            public DateOnly DefaultDateOnly { get; set; }
            public DateTimeOffset DefaultDateTimeOffset { get; set; }
        }
        public TestIndex()
        {
            Map = collection => from c in collection
                select new Result
                {
                    DefaultDateTime = default,
                    DefaultTimeOnly = default,
                    DefaultDateOnly = default,
                    DefaultDateTimeOffset = default
                };
            
            StoreAllFields(FieldStorage.Yes);
        }
    }
}
