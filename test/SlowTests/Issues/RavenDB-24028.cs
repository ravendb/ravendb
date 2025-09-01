using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Documents.Operations.Indexes;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_24028 : RavenTestBase
{
    public RavenDB_24028(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Indexes)]
    public async Task ArrayDoesNotContainDefinitionForWhere()
    {
        using (var store = GetDocumentStore())
        {
            await store.ExecuteIndexAsync(new MyIndex());
            await store.ExecuteIndexAsync(new MyIndexKnownTypesArrays());
            await store.ExecuteIndexAsync(new MyIndexKnownTypesLists());
            await store.ExecuteIndexAsync(new MyIndexKnownTypesEnumerables());
            
            using (var session = store.OpenSession())
            {
                session.Store(new TestDocument { Name = "Peter", AnyId = null });
                session.SaveChanges();
            }

            await Indexes.WaitForIndexingAsync(store);

            var indexes = await store.Maintenance.SendAsync(new GetIndexErrorsOperation());
            var erroredIndexes = indexes
                .Where(i => i.Errors.Any())
                .ToList();

            Assert.Empty(erroredIndexes);
        }
    }

    private class TestDocument
    {
        public string Name { get; set; }
        public string AnyId { get; set; }
    }

    private class MyIndex : AbstractIndexCreationTask<TestDocument>
    {
        public MyIndex()
        {
            Map = documents => from document in documents
                let e1 = document.AnyId != null ? AttachmentsFor(document) : new AttachmentName[0]
                let e2 = document.AnyId == null ? new AttachmentName[0] : AttachmentsFor(document)
                
                let e3 = document.AnyId != null ? AttachmentsFor(document) : new AttachmentName[] { }
                let e4 = document.AnyId == null ? new AttachmentName[] { } : AttachmentsFor(document)
                
                let e5 = document.AnyId != null ? AttachmentsFor(document) : Array.Empty<AttachmentName>()
                let e6 = document.AnyId == null ? Array.Empty<AttachmentName>() : AttachmentsFor(document)

                let e7 = document.AnyId != null ? AttachmentsFor(document) : new List<AttachmentName>()
                let e8 = document.AnyId == null ? new List<AttachmentName>() : AttachmentsFor(document)
                
                let e9 = document.AnyId != null ? AttachmentsFor(document) : Enumerable.Empty<AttachmentName>()
                let e10 = document.AnyId == null ? Enumerable.Empty<AttachmentName>() : AttachmentsFor(document)

                let e11 = new AttachmentName[0]
                let e12 = new AttachmentName[] { }
                let e13 = Array.Empty<AttachmentName>()
                let e14 = new List<AttachmentName>()
                let e15 = Enumerable.Empty<AttachmentName>()

                select new
                {
                    l1 = e1
                        .Where(a => a.ContentType == "application/pdf")
                        .ToList(),
                    l2 = e2
                        .Where(a => a.ContentType == "application/pdf")
                        .ToList(),
                    l3 = e3
                        .Where(a => a.ContentType == "application/pdf")
                        .ToList(),
                    l4 = e4
                        .Where(a => a.ContentType == "application/pdf")
                        .ToList(),
                    l5 = e5
                        .Where(a => a.ContentType == "application/pdf")
                        .ToList(),
                    l6 = e6
                        .Where(a => a.ContentType == "application/pdf")
                        .ToList(),
                    l7 = e7
                        .Where(a => a.ContentType == "application/pdf")
                        .ToList(),
                    l8 = e8
                        .Where(a => a.ContentType == "application/pdf")
                        .ToList(),
                    l9 = e9
                        .Where(a => a.ContentType == "application/pdf")
                        .ToList(),
                    l10 = e10
                        .Where(a => a.ContentType == "application/pdf")
                        .ToList(),
                    l11 = e11
                        .Where(a => a.ContentType == "application/pdf")
                        .ToList(),
                    l12 = e12
                        .Where(a => a.ContentType == "application/pdf")
                        .ToList(),
                    l13 = e13
                        .Where(a => a.ContentType == "application/pdf")
                        .ToList(),
                    l14 = e13
                        .Where(a => a.ContentType == "application/pdf")
                        .ToList(),
                    l15 = e15
                        .Where(a => a.ContentType == "application/pdf")
                        .ToList()
                };

            StoreAllFields(FieldStorage.Yes);
        }
    }
    
    private class MyIndexKnownTypesArrays : AbstractMultiMapIndexCreationTask
    {
        public MyIndexKnownTypesArrays()
        {
            AddMap<TestDocument>(documents => from document in documents
                let e = document.AnyId != null ? new []{ "abc" } : new string[0]
                select new 
                {
                    l = e
                        .Where(a => a.StartsWith("application/pdf"))
                        .ToList(),
                });
            
            AddMap<TestDocument>(documents => from document in documents
                let e = document.AnyId == null ? new string[0] : new []{ "abc" }
                select new 
                {
                    l = e
                        .Where(a => a.StartsWith("application/pdf"))
                        .ToList(),
                });
            
            AddMap<TestDocument>(documents => from document in documents
                let e = document.AnyId != null ? new []{ "abc" } : new string[] { }
                select new 
                {
                    l = e
                        .Where(a => a.StartsWith("application/pdf"))
                        .ToList(),
                });
            
            AddMap<TestDocument>(documents => from document in documents
                let e = document.AnyId == null ? new string[] { } : new []{ "abc" }
                select new 
                {
                    l = e
                        .Where(a => a.StartsWith("application/pdf"))
                        .ToList(),
                });
            
            AddMap<TestDocument>(documents => from document in documents
                let e = document.AnyId != null ? new []{ "abc" } : Array.Empty<string>()
                select new 
                {
                    l = e
                        .Where(a => a.StartsWith("application/pdf"))
                        .ToList(),
                });
            
            AddMap<TestDocument>(documents => from document in documents
                let e = document.AnyId == null ? Array.Empty<string>() : new []{ "abc" }
                select new 
                {
                    l = e
                        .Where(a => a.StartsWith("application/pdf"))
                        .ToList(),
                });
            
            AddMap<TestDocument>(documents => from document in documents
                let e = new string[0]
                select new 
                {
                    l = e
                        .Where(a => a.StartsWith("application/pdf"))
                        .ToList(),
                });
            
            AddMap<TestDocument>(documents => from document in documents
                let e = new string[] { }
                select new 
                {
                    l = e
                        .Where(a => a.StartsWith("application/pdf"))
                        .ToList(),
                });
            
            AddMap<TestDocument>(documents => from document in documents
                let e = Array.Empty<string>()
                select new 
                {
                    l = e
                        .Where(a => a.StartsWith("application/pdf"))
                        .ToList(),
                });
            
            StoreAllFields(FieldStorage.Yes);
        }
    }
    
    private class MyIndexKnownTypesLists : AbstractMultiMapIndexCreationTask
    {
        public MyIndexKnownTypesLists()
        {
            AddMap<TestDocument>(documents => from document in documents
                let e = document.AnyId != null ? new List<string>{ "abc" } : new List<string>()
                select new 
                {
                    l = e
                        .Where(a => a.StartsWith("application/pdf"))
                        .ToList(),
                });
            
            AddMap<TestDocument>(documents => from document in documents
                let e = document.AnyId == null ? new List<string>() : new List<string>{ "abc" }
                select new 
                {
                    l = e
                        .Where(a => a.StartsWith("application/pdf"))
                        .ToList(),
                });
            
            AddMap<TestDocument>(documents => from document in documents
                let e = new List<string>()
                select new 
                {
                    l = e
                        .Where(a => a.StartsWith("application/pdf"))
                        .ToList(),
                });
            
            StoreAllFields(FieldStorage.Yes);
        }
    }

    private class MyIndexKnownTypesEnumerables : AbstractMultiMapIndexCreationTask
    {
        public MyIndexKnownTypesEnumerables()
        {
            AddMap<TestDocument>(documents => from document in documents
                let e = document.AnyId != null ? Enumerable.Repeat("abc", 1) : Enumerable.Empty<string>()
                select new 
                {
                    l = e
                        .Where(a => a.StartsWith("application/pdf"))
                        .ToList(),
                });
            
            AddMap<TestDocument>(documents => from document in documents
                let e = document.AnyId == null ? Enumerable.Empty<string>() : Enumerable.Repeat("abc", 1)
                select new 
                {
                    l = e
                        .Where(a => a.StartsWith("application/pdf"))
                        .ToList(),
                });
            
            AddMap<TestDocument>(documents => from document in documents
                let e = Enumerable.Empty<string>()
                select new 
                {
                    l = e
                        .Where(a => a.StartsWith("application/pdf"))
                        .ToList(),
                });
        }
    }
}
