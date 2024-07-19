using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.Documents.Indexes.Static.Linq;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_22615 : RavenTestBase
{
    public RavenDB_22615(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Indexes)]
    public void TestIndexWithListInitExpression()
    {
        using (var store = GetDocumentStore())
        {
            var index = new DummyIndex();
            
            index.Execute(store);

            using (var session = store.OpenSession())
            {
                var dto1 = new Dto() { Names = new List<string>(){ "Name1", "Name2" } };
                var dto2 = new Dto() { Names = new List<string>(){ "Name3" } };
                
                session.Store(dto1);
                session.Store(dto2);
                
                session.SaveChanges();
                
                Indexes.WaitForIndexing(store);

                var res = session.Query<DummyIndex.IndexEntry, DummyIndex>().Where(x => x.StringList.Contains("Name1")).ProjectInto<Dto>().ToList();

                Assert.Equal(1, res.Count);
                Assert.Equal(dto1.Id, res[0].Id);

                /*
                var terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, nameof(DummyIndex.IndexEntry.SomeClassList), null));

                Assert.Contains("{\"name\":\"name1\"}", terms);
                Assert.Contains("{\"name\":\"name2\"}", terms);
                Assert.Contains("{\"name\":\"name3\"}", terms);
                
                res = session.Query<DummyIndex.IndexEntry, DummyIndex>().Where(x => x.FirstElementOfStringList == "Name3").ProjectInto<Dto>().ToList();
                
                Assert.Equal(1, res.Count);
                Assert.Equal(dto2.Id, res[0].Id);
                
                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, nameof(DummyIndex.IndexEntry.FirstElementOfSomeClassList), null));

                Assert.Contains("{\"name\":\"name1\"}", terms);
                Assert.Contains("{\"name\":\"name2\"}", terms);
                Assert.Contains("{\"name\":\"name3\"}", terms);
                */
            }
        }
    }
    
    private class DummyIndex : AbstractMultiMapIndexCreationTask<Dto>
    {
        public class IndexEntry
        {
            public List<string> StringList { get; set; }
            //public List<SomeClass> SomeClassList { get; set; }
            //public string FirstElementOfStringList { get; set; }
            //public SomeClass FirstElementOfSomeClassList { get; set; }
        }
        public DummyIndex()
        {
            AddMap<Dto>(dtos => from dto in dtos
                from name in dto.Names
                let otherName = "blabla"
                select new IndexEntry()
                {
                    StringList = new List<string> { name, otherName },
                    //SomeClassList = new List<SomeClass>() { new SomeClass() { Name = name } },
                    //FirstElementOfStringList = new List<string>() { name }.First(),
                    //FirstElementOfSomeClassList = new List<SomeClass>() { new SomeClass() { Name = name } }.First()
                });
            
            StoreAllFields(FieldStorage.Yes);
        }
    }

    private class Dto
    {
        public string Id { get; set; }
        public List<string> Names { get; set; }
    }

    private class SomeClass
    {
        public string Name { get; set; }
    }
    
    [RavenFact(RavenTestCategory.Indexes)]
    public void TestMultiMapReduceIndex()
    {
        using (var store = GetDocumentStore())
        {
            using (var session = store.OpenSession())
            {
                var property = new Property() { Status = 2137 };
                
                session.Store(property);
                
                var propertyImage = new PropertyImage() { PropertyId = property.Id, Url = "SomeUrl" };
                
                session.Store(propertyImage);
                
                session.SaveChanges();
                
                var index = new PropertiesIndex();
                
                index.Execute(store);
            
                Indexes.WaitForIndexing(store);

                var res = session.Query<PropertyIndexResult, PropertiesIndex>().ToList();
                
                Assert.Equal(property.Id, res[0].Id);
            }
        }
    }
    
    private class PropertiesIndex : AbstractMultiMapIndexCreationTask<PropertyIndexResult>
    {
        public PropertiesIndex()
        {
            AddMap<Property>(properties => from property in properties.Where(x => x.Status < 2)
                select new PropertyIndexResult
                {
                    Id = property.Id,
                    Images = new List<PropertyImage>()
                });
            AddMap<PropertyImage>(images => from image in images
                select new PropertyIndexResult
                {
                    Id = image.PropertyId,
                    Images = new List<PropertyImage> { image }
                });
    
            Reduce = results => from result in results
                group result by result.Id into g
                select new PropertyIndexResult
                {
                    Id = g.Key,
                    Images = g.SelectMany(x => x.Images ?? new List<PropertyImage>()).ToList()
                };
            StoreAllFields(FieldStorage.Yes);
        }
    }
    
    private class Property
    {
        public long Status { get; set; }
        public string Id { get; set; }
    }
    private class PropertyImage
    {
        public string PropertyId { get; set; }
        public string? Url { get; set; }
    }
    private class PropertyIndexResult
    {
        public string Id { get; set; }
        public List<PropertyImage>? Images { get; set; } = [];
    }
    
    [RavenFact(RavenTestCategory.Indexes)]
    public void TestIndexUsingDictWithCustomClass()
    {
        using (var store = GetDocumentStore())
        {
            using (var session = store.OpenSession())
            {
                var dto1 = new Dto() { Names = new List<string>() { "Name1", "Name2" } };
                var dto2 = new Dto() { Names = new List<string>() { "Name3" } };
                
                session.Store(dto1);
                session.Store(dto2);
                
                session.SaveChanges();
                
                var index = new IndexWithDict();
                
                index.Execute(store);
                
                Indexes.WaitForIndexing(store);
                
                var terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, nameof(IndexWithDict.IndexEntry.SomeDict), null));
                
                Assert.Equal(3, terms.Length);
                Assert.Equal("{\"name1\":{\"@metadata\":{\"@collection\":\"dtos\",\"raven-clr-type\":\"slowtests.issues.ravendb_22615+dto, slowtests\"},\"names\":[\"name1\",\"name2\"]}}", terms[0]);
                Assert.Equal("{\"name2\":{\"@metadata\":{\"@collection\":\"dtos\",\"raven-clr-type\":\"slowtests.issues.ravendb_22615+dto, slowtests\"},\"names\":[\"name1\",\"name2\"]}}", terms[1]);
                Assert.Equal("{\"name3\":{\"@metadata\":{\"@collection\":\"dtos\",\"raven-clr-type\":\"slowtests.issues.ravendb_22615+dto, slowtests\"},\"names\":[\"name3\"]}}", terms[2]);
            }
        }
    }

    private class IndexWithDict : AbstractIndexCreationTask<Dto>
    {
        public class IndexEntry
        {
            public Dictionary<string, Dto> SomeDict { get; set; }
        }
        
        public IndexWithDict()
        {
            Map = dtos => from dto in dtos
                from name in dto.Names
                select new IndexEntry() { SomeDict = new Dictionary<string, Dto>() { { name, dto } } };
        }
    }
}
