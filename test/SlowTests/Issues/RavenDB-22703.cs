using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Backups;
using Sparrow.Json.Parsing;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_22703 : RavenTestBase
{
    public RavenDB_22703(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Indexes)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void TestQueryWithOrderByClauseAndNoWhereClause(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            using (var session = store.OpenSession())
            {
                var bar1 = new Bar() { Foo = new Foo() { BarBool = false, BarShort = 9 } };
                var bar2 = new Bar() { Foo = new Foo() { BarBool = null } };
                var bar3 = new Bar() { Foo = null };
                
                session.Store(bar1);
                session.Store(bar2);
                session.Store(bar3);
                
                session.SaveChanges();
                
                var requestExecutor = store.GetRequestExecutor();
                using (requestExecutor.ContextPool.AllocateOperationContext(out var context))
                {
                    var reader = context.ReadObject(new DynamicJsonValue
                    {
                        ["@metadata"] = new DynamicJsonValue{
                            ["@collection"] = "Bars",
                            ["Raven-Clr-Type"] = "SlowTests.Issues.RavenDB_22703+Bar, SlowTests"
                        }
                    }, "bars/4-A");
                    requestExecutor.Execute(new PutDocumentCommand(store.Conventions, "bars/4-A", null, reader), context);
                }
                
                var res = session.Query<Bar>()
                    .OrderByDescending(b => b.Foo.BarBool)
                    .ThenByDescending(b => b.Foo.BarShort)
                    .ToList();

                Assert.Equal(4, res.Count);
                
                var deleteByQueryOp = store.Operations.Send(new DeleteByQueryOperation("from 'Bars'"));
                
                deleteByQueryOp.WaitForCompletion(TimeSpan.FromMinutes(1));
                
                Indexes.WaitForIndexing(store);
                
                res = session.Query<Bar>()
                    .OrderByDescending(b => b.Foo.BarBool)
                    .ThenByDescending(b => b.Foo.BarShort)
                    .ToList();
                
                Assert.Equal(0, res.Count);
            }
        }
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Indexes)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.Sharded)]
    public void TestOrderingOnShardedDatabase(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            using (var session = store.OpenSession())
            {
                var bar1 = new Bar() { Foo = new Foo() { BarBool = false, BarShort = 9 } };
                var bar2 = new Bar() { Foo = new Foo() { BarBool = null } };
                var bar3 = new Bar() { Foo = null };
                var bar4 = new Bar() { Foo = new Foo() { BarBool = true, BarShort = 21 } };
                var bar5 = new Bar() { Foo = null };
                
                session.Store(bar1);
                session.Store(bar2);
                session.Store(bar3);
                session.Store(bar4);
                session.Store(bar5);
                
                session.SaveChanges();
                
                var requestExecutor = store.GetRequestExecutor();
                using (requestExecutor.ContextPool.AllocateOperationContext(out var context))
                {
                    var reader = context.ReadObject(new DynamicJsonValue
                    {
                        ["@metadata"] = new DynamicJsonValue{
                            ["@collection"] = "Bars",
                            ["Raven-Clr-Type"] = "SlowTests.Issues.RavenDB_22703+Bar, SlowTests"
                        }
                    }, "bars/6");
                    requestExecutor.Execute(new PutDocumentCommand(store.Conventions, "bars/6", null, reader), context);
                }
                
                var res = session.Query<Bar>()
                    .OrderByDescending(b => b.Foo.BarBool)
                    .ThenByDescending(b => b.Foo.BarShort)
                    .ToList();
                
                Assert.Equal(6, res.Count);
                
                Assert.Equal(true, res[0].Foo.BarBool);
                Assert.Equal(false, res[1].Foo.BarBool);
            }
        }
    }
    
    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Indexes)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.Single)]
    public void TestIndexVersionCheck(Options options)
    {
        var backupPath = NewDataPath(forceCreateDir: true);
        var file = Path.Combine(backupPath, "RavenDB_22703.ravendb-snapshot");
        
        using (var fileStream = File.Create(file))
        using (var stream = typeof(RavenDB_10404).Assembly.GetManifestResourceStream("SlowTests.Data.RavenDB_22703.RavenDB_22703.ravendb-snapshot"))
        {
            stream.CopyTo(fileStream);
        }

        using (var store = GetDocumentStore(options))
        {
            var databaseName = GetDatabaseName();
            using (var _ = Backup.RestoreDatabase(store, new RestoreBackupConfiguration { BackupLocation = backupPath, DatabaseName = databaseName }))
            {
                using (var session = store.OpenSession(databaseName))
                {
                    var res = session.Query<Bars_ByBarBoolAndBarShort.IndexEntry, Bars_ByBarBoolAndBarShort>()
                        .OrderByDescending(x => x.BarBool)
                        .ThenByDescending(x => x.BarShort)
                        .ProjectInto<Bar>()
                        .ToList();

                    Assert.Equal(3, res.Count);
                    Assert.Equal(true, res[0].Foo.BarBool);
                    Assert.Equal(false, res[1].Foo.BarBool);
                    Assert.Equal(null, res[2].Foo.BarBool);
                }
            }
        }
    }

    private class Bars_ByBarBoolAndBarShort : AbstractIndexCreationTask<Bar>
    {
        public class IndexEntry
        {
            public bool? BarBool { get; set; }
            public short BarShort { get; set; }
        }
        public Bars_ByBarBoolAndBarShort()
        {
            Map = bars => from bar in bars
                select new IndexEntry() { BarBool = bar.Foo.BarBool, BarShort = bar.Foo.BarShort };
        }
    }
    
    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Indexes)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All)]
    public void TestStaticIndex(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            using (var session = store.OpenSession())
            {
                var subDto1 = new SubDto() { Name = "FirstName" };
                var subDto2 = new SubDto() { Name = "SecondName" };
                
                var dto1 = new Dto() { SubDtoFirst = subDto1, SubDtoSecond = subDto2 };
                
                session.Store(dto1);

                session.SaveChanges();
                
                var requestExecutor = store.GetRequestExecutor();
                using (requestExecutor.ContextPool.AllocateOperationContext(out var context))
                {
                    var reader = context.ReadObject(new DynamicJsonValue
                    {
                        ["SubDtoFirst"] = new DynamicJsonValue()
                        {
                            ["Name"] = "FirstName"
                        },
                        ["@metadata"] = new DynamicJsonValue{
                            ["@collection"] = "Dtos",
                            ["Raven-Clr-Type"] = "SlowTests.Issues.RavenDB_22703+Dto, SlowTests"
                        }
                    }, "dtos/2");
                    requestExecutor.Execute(new PutDocumentCommand(store.Conventions, "dtos/2", null, reader), context);
                    
                    reader = context.ReadObject(new DynamicJsonValue
                    {
                        ["@metadata"] = new DynamicJsonValue{
                            ["@collection"] = "Dtos",
                            ["Raven-Clr-Type"] = "SlowTests.Issues.RavenDB_22703+Dto, SlowTests"
                        }
                    }, "dtos/3");
                    requestExecutor.Execute(new PutDocumentCommand(store.Conventions, "dtos/3", null, reader), context);
                }
                
                var index = new Dto_BySubDtos();
                
                index.Execute(store);
                
                Indexes.WaitForIndexing(store);
                
                var res = session.Query<Dto_BySubDtos.IndexEntry, Dto_BySubDtos>()
                    .OrderByDescending(x => x.FirstName)
                    .ThenByDescending(x => x.SecondName)
                    .ProjectInto<Dto>()
                    .ToList();
                
                Assert.Equal(2, res.Count);

                index.Configuration["Indexing.IndexEmptyEntries"] = "true";
                
                index.Execute(store);
                
                Indexes.WaitForIndexing(store);
                
                res = session.Query<Dto_BySubDtos.IndexEntry, Dto_BySubDtos>()
                    .OrderByDescending(x => x.FirstName)
                    .ThenByDescending(x => x.SecondName)
                    .ProjectInto<Dto>()
                    .ToList();
                
                Assert.Equal(3, res.Count);
            }
        }
    }

    private class Bar
    {
        public Foo Foo { get; set; } = null!;
    }

    private class Foo
    {
        public short BarShort { get; set; }
        public bool? BarBool { get; set; }
    }

    private class Dto
    {
        public SubDto SubDtoFirst { get; set; }
        public SubDto SubDtoSecond { get; set; }
    }

    private class SubDto
    {
        public string Name { get; set; }
    }

    private class Dto_BySubDtos : AbstractIndexCreationTask<Dto>
    {
        public class IndexEntry
        {
            public string FirstName { get; set; }
            public string SecondName { get; set; }
        }
        public Dto_BySubDtos()
        {
            Map = dtos => from dto in dtos
                select new IndexEntry() { FirstName = dto.SubDtoFirst.Name, SecondName = dto.SubDtoSecond.Name };
        }
    }
    
    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Indexes)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All)]
    public void TestOrderingOfDynamicFields(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            using (var session = store.OpenSession())
            {
                var p1 = new Product() { Attributes = new Dictionary<string, object>() { { "Color", "Red" }, {"Size", 42 } } };
                var p2 = new Product() { Attributes = new Dictionary<string, object>() { { "Color", "Blue" } } };
                var p3 = new Product() { Attributes = new Dictionary<string, object>() { { "Size", 37 } } };
                
                session.Store(p1);
                session.Store(p2);
                session.Store(p3);
                
                session.SaveChanges();
                
                var index = new Products_ByAttributeKey();
                
                index.Execute(store);
                
                Indexes.WaitForIndexing(store);
                
                var res = session
                    .Advanced
                    .DocumentQuery<Product, Products_ByAttributeKey>()
                    .OrderBy("Size")
                    .ToList();

                Assert.Equal(3, res.Count);
            }
        }
    }

    private class Product
    {
        public Dictionary<string, object> Attributes { get; set; }
    }
    
    private class Products_ByAttributeKey : AbstractIndexCreationTask<Product>
    {
        public Products_ByAttributeKey()
        {
            Map = products => from p in products
                select new
                {
                    _ = p.Attributes.Select(item => CreateField(item.Key, item.Value))
                };
        }
    }
}
