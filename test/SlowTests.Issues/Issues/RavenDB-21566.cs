using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Indexes;
using Raven.Client.Exceptions.Documents.Compilation;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_21566 : RavenTestBase
{
    public RavenDB_21566(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Indexes)]
    public void CheckIfGetValueOrDefaultIsReplacedWithNullCoalescence()
    {
        using (var store = GetDocumentStore())
        {
            using (var session = store.OpenSession())
            {
                var d1 = new Dto();
                
                session.Store(d1);
                
                session.SaveChanges();

                var index = new DummyIndex();
                index.Execute(store);
                
                Indexes.WaitForIndexing(store);

                var res = session.Query<DummyIndex.IndexResult>(index.IndexName).ProjectInto<DummyIndex.IndexResult>().ToList();

                var firstResult = res.First();
                
                Assert.Equal(default, firstResult.NonNullableBool);
                Assert.Equal(default, firstResult.NonNullableInt);
                Assert.Equal(default, firstResult.NonNullableDecimal);
                Assert.Equal(default, firstResult.NonNullableDouble);
                Assert.Equal(default, firstResult.NonNullableFloat);
            }
        }
    }
    
    [RavenFact(RavenTestCategory.Indexes)]
    public void CheckIfTypeAddedThroughAdditionalSourcesWorksCorrectly()
    {
        using (var store = GetDocumentStore())
        {
            using (var session = store.OpenSession())
            {
                var d1 = new DtoWithNullableEnum();
                
                session.Store(d1);
                
                session.SaveChanges();
                
                var conventions = new DocumentConventions
                {
                    TypeIsKnownServerSide = t => t == typeof(DummyEnum)
                };

                var index = new DummyIndexWithNullableEnum() { Conventions = conventions };
                
                index.Execute(store);
                
                Indexes.WaitForIndexing(store);
                
                var res = session.Query<DummyIndexWithNullableEnum.IndexResult>(index.IndexName).ProjectInto<DummyIndexWithNullableEnum.IndexResult>().ToList();
                
                var firstResult = res.First();
                
                Assert.Equal(default, firstResult.NonNullableEnum);
            }
        }
    }

    [RavenFact(RavenTestCategory.Indexes)]
    public void CheckIfTypeThatDoesNotExistOnServerThrows()
    {
        using (var store = GetDocumentStore())
        {
            using (var session = store.OpenSession())
            {
                var d1 = new DtoWithNullableEnum();
                
                session.Store(d1);
                
                session.SaveChanges();

                // We don't register DummyEnum type on server
                var index = new DummyIndexWithNullableEnum();
                
                var exception = Assert.Throws<IndexCompilationException>(() => index.Execute(store));

                var innerException = exception.InnerException;
                
                Assert.IsType<InvalidOperationException>(innerException);

                Assert.Contains($"Type SlowTests.Issues.RavenDB_21566+DummyEnum does not exist on server, default value cannot be assigned. Did you intend to register it via {nameof(DocumentConventions.TypeIsKnownServerSide)} convention?", innerException.Message);
            }
        }
    }

    [RavenFact(RavenTestCategory.Indexes)]
    public void CheckIfNullableDateTypesAreEvaluatedCorrectly()
    {
        using (var store = GetDocumentStore())
        {
            using (var session = store.OpenSession())
            {
                var d1 = new DtoWithNullableDates();
                
                session.Store(d1);
                
                session.SaveChanges();

                var index = new DummyIndexWithNullableDates();
                
                index.Execute(store);
                
                Indexes.WaitForIndexing(store);
                
                var res = session.Query<DummyIndexWithNullableDates.IndexResult>(index.IndexName).ProjectInto<DummyIndexWithNullableDates.IndexResult>().ToList();
                
                var firstResult = res.First();
                
                Assert.Equal(default, firstResult.NonNullableDateOnly);
                Assert.Equal(default, firstResult.NonNullableTimeSpan);
                Assert.Equal(default, firstResult.NonNullableDateTimeOffset);
                Assert.Equal(default, firstResult.NonNullableDateOnly);
                Assert.Equal(default, firstResult.NonNullableTimeOnly);
            }
        }
    }

    private class Dto
    {
        public string Id { get; set; }
        public bool? NullableBool { get; set; }
        public int? NullableInt { get; set; }
        public decimal? NullableDecimal { get; set; }
        public double? NullableDouble { get; set; }
        public float? NullableFloat { get; set; }
    }

    private class DtoWithNullableEnum
    {
        public DummyEnum? NullableEnum { get; set; }
    }

    private class DtoWithNullableDates
    {
        public DateTime? NullableDateTime { get; set; }
        public TimeSpan? NullableTimeSpan { get; set; }
        public DateTimeOffset? NullableDateTimeOffset { get; set; }
        public DateOnly? NullableDateOnly { get; set; }
        public TimeOnly? NullableTimeOnly { get; set; }
    }
    
    private enum DummyEnum
    {
        Value1,
        Value2
    }

    private class DummyIndex : AbstractIndexCreationTask<Dto>
    {
        public class IndexResult
        {
            public string Id { get; set; }
            public bool NonNullableBool { get; set; }
            public int NonNullableInt { get; set; }
            public decimal NonNullableDecimal { get; set; }
            public double NonNullableDouble { get; set; }
            public float NonNullableFloat { get; set; }
        }
        
        public DummyIndex()
        {
            Map = dtos => from dto in dtos
                select new IndexResult()
                {
                    Id = dto.Id, 
                    NonNullableBool = dto.NullableBool.GetValueOrDefault(),
                    NonNullableInt = dto.NullableInt.GetValueOrDefault(),
                    NonNullableDecimal = dto.NullableDecimal.GetValueOrDefault(),
                    NonNullableDouble = dto.NullableDouble.GetValueOrDefault(),
                    NonNullableFloat = dto.NullableFloat.GetValueOrDefault()
                };
            
            StoreAllFields(FieldStorage.Yes);
        }
    }
    
    private class DummyIndexWithNullableEnum : AbstractIndexCreationTask<DtoWithNullableEnum>
    {
        public class IndexResult
        {
            public DummyEnum NonNullableEnum { get; set; }
        }
        
        public DummyIndexWithNullableEnum()
        {
            Map = dtos => from dto in dtos
                select new IndexResult()
                {
                    NonNullableEnum = dto.NullableEnum.GetValueOrDefault()
                };
            
            StoreAllFields(FieldStorage.Yes);
            
            AdditionalSources = new Dictionary<string, string>
            {
                {
                    "DummyAdditionalSource", 
                    @"
                    using System;

                    namespace Temp 
                    {
                        public enum DummyEnum 
                        {
                            Value1,
                            Value2
                        }
                    }"
                }
            };
        }
    }
    private class DummyIndexWithNullableDates : AbstractIndexCreationTask<DtoWithNullableDates>
    {
        public class IndexResult
        {
            public DateTime NonNullableDateTime { get; set; }
            public TimeSpan NonNullableTimeSpan { get; set; }
            public DateTimeOffset NonNullableDateTimeOffset { get; set; }
            public DateOnly NonNullableDateOnly { get; set; }
            public TimeOnly NonNullableTimeOnly { get; set; }
        }
        
        public DummyIndexWithNullableDates()
        {
            Map = dtos => from dto in dtos
                select new IndexResult()
                {
                    NonNullableDateTime = dto.NullableDateTime.GetValueOrDefault(),
                    NonNullableTimeSpan = dto.NullableTimeSpan.GetValueOrDefault(),
                    NonNullableDateTimeOffset = dto.NullableDateTimeOffset.GetValueOrDefault(),
                    NonNullableDateOnly = dto.NullableDateOnly.GetValueOrDefault(),
                    NonNullableTimeOnly = dto.NullableTimeOnly.GetValueOrDefault()
                };
            
            StoreAllFields(FieldStorage.Yes);
        }
    }
}
