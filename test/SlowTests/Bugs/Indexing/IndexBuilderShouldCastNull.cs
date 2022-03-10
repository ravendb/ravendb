using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Indexes;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Bugs.Indexing
{
    public class IndexBuilderShouldCastNull : RavenTestBase
    {
        public IndexBuilderShouldCastNull(ITestOutputHelper output) : base(output)
        {
        }

        [Theory]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.Lucene)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Skip = "RavenDB-17966")]
        public void ShouldCastNullToThePropertyType(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                new NullableIndex().Execute(store);
            }
        }

        [Theory]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.Lucene)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Skip = "RavenDB-17966")]
        public void ShouldWorkAlsoWithAnonymousResultTypeWhichRequiredExplicitlyCast(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                new AnonymousNullableIndex().Execute(store);
            }
        }

        [Fact]
        public void NullableIndexDoesNotCastTwice()
        {
            var indexDefinition = new NullableIndex { Conventions = new DocumentConventions() }.CreateIndexDefinition();
            Assert.False(indexDefinition.Maps.Contains("(string)(string)")); // Include also (String)(string)|(string)(String) cases.
            Assert.False(indexDefinition.Maps.Contains("(System.String)(string)"));
            Assert.False(indexDefinition.Maps.Contains("(string)(System.String)"));
            Assert.False(indexDefinition.Maps.Contains("(System.String)(System.String)"));
        }

        [Fact]
        public void AnonymousNullableIndexDoesNotCastTwice()
        {
            var indexDefinition = new AnonymousNullableIndex { Conventions = new DocumentConventions() }.CreateIndexDefinition();
            Assert.False(indexDefinition.Maps.Contains("(string)(string)")); // Include also (String)(string)|(string)(String) cases.
            Assert.False(indexDefinition.Maps.Contains("(System.String)(string)"));
            Assert.False(indexDefinition.Maps.Contains("(string)(System.String)"));
            Assert.False(indexDefinition.Maps.Contains("(System.String)(System.String)"));
        }

        private class Nullable
        {
            public char? Char { get; set; }
            public string String { get; set; }
            public object Object { get; set; }
            public decimal? Decimal { get; set; }
            public double? Double { get; set; }
            public float? Float { get; set; }
            public long? Long { get; set; }
            public int? Int { get; set; }
            public short? Short { get; set; }
            public byte? Byte { get; set; }
            public bool? Bool { get; set; }
            public DateTime? DateTime { get; set; }
            public DateTimeOffset? DateTimeOffset { get; set; }
            public TimeSpan? TimeSpan { get; set; }
            public Guid? Guid { get; set; }
        }

        private class Result : Nullable
        {
        }

        private class NullableIndex : AbstractMultiMapIndexCreationTask<Result>
        {
            public NullableIndex()
            {
                AddMap<Nullable>(nullables => nullables.Select(nullable => new Result
                {
                    Char = null,
                    String = null,
                    Object = null,
                    Decimal = null,
                    Double = null,
                    Float = null,
                    Long = null,
                    Int = null,
                    Short = null,
                    Byte = null,
                    Bool = null,
                    DateTime = null,
                    DateTimeOffset = null,
                    TimeSpan = null,
                    Guid = null,
                }));
            }
        }

        private class AnonymousNullableIndex : AbstractMultiMapIndexCreationTask<Result>
        {
            public AnonymousNullableIndex()
            {
                AddMap<Nullable>(nullables => nullables.Select(nullable => new
                {
                    Char = (char?)null,
                    String = (string)null,
                    Object = (object)null,
                    Decimal = (decimal?)null,
                    Double = (double?)null,
                    Float = (float?)null,
                    Long = (long?)null,
                    Int = (int?)null,
                    Short = (short?)null,
                    Byte = (byte?)null,
                    Bool = (bool?)null,
                    DateTime = (DateTime?)null,
                    DateTimeOffset = (DateTimeOffset?)null,
                    TimeSpan = (TimeSpan?)null,
                    Guid = (Guid?)null,
                }));
            }
        }
    }
}
