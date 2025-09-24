using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Sparrow;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_16190 : RavenTestBase
{
    public RavenDB_16190(ITestOutputHelper output) : base(output)
    {
    }
    
    [RavenTheory(RavenTestCategory.Querying)]
    [InlineData(@"from index 'Qs' as o select Content")]
    [InlineData(@"from index 'MultipleAttachmentsIndex' as o select Content")]
    public void CheckIfCanGetMultipleAttachmentsFromDocument(string query)
    {
        using var store = GetDocumentStore();

        using var file1 = new MemoryStream();
        using var file2 = new MemoryStream();
        
        file1.Write(Encodings.Utf8.GetBytes(new string('a', 5)).AsSpan());
        file2.Write(Encodings.Utf8.GetBytes(new string('b', 4)).AsSpan());
        
        file1.Position = 0;
        file2.Position = 0;
        
        using (var session = store.OpenSession())
        {
            Order o1 = new() { Price = 21 };

            session.Store(o1);

            session.Advanced.Attachments.Store(o1.Id, "f1.txt", file1);
            session.Advanced.Attachments.Store(o1.Id, "f2.txt", file2);

            session.SaveChanges();

            var index = new MultipleAttachmentsIndex();
            index.Execute(store);
            
            var qs = new Qs();
            qs.Execute(store);
            
            Indexes.WaitForIndexing(store);
            
            WaitForUserToContinueTheTest(store);
            
            var res = session.Advanced
                .RawQuery<Order>(query)
                .WaitForNonStaleResults().ToList();
            
            Assert.Equal(res[0].Content, "aaaaabbbb");
        }
    }

    private class MultipleAttachmentsIndex : AbstractIndexCreationTask<Order>
    {
        public MultipleAttachmentsIndex()
        {
            Map = orders => from o in orders
                let attachments = LoadAttachments(o).Select(x => x.GetContentAsString())
                select new { Content = attachments.Aggregate((s, s1) => s + s1) };
            Stores.Add(x => x.Content, FieldStorage.Yes);
        }
    }

    private class Qs : AbstractIndexCreationTask
    {
        public Qs()
        {
            
        }

        public override IndexDefinition CreateIndexDefinition()
        {
            return new IndexDefinition()
            {
                Maps = new()
                {
                    @"from o in docs.Orders
                let attachments = LoadAttachments(o).Select(x => x.GetContentAsString())
                select new { Content = attachments.Aggregate((s, s1) => s + s1) }"
                },
                Fields = new Dictionary<string, IndexFieldOptions>(){ { "Content", new IndexFieldOptions() { Storage = FieldStorage.Yes } } }
            };
        }
    }
    
    private class Order
    {
        public string Id { get; set; }
        public int Price { get; set; }
        public string Content { get; set; }
    }
}
