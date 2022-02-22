//-----------------------------------------------------------------------
// <copyright file="UsingDynamicQueryWithLocalServer.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries.Highlighting;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Tests.Issues;

public class RavenDB18078 : RavenTestBase
{
    public RavenDB18078(ITestOutputHelper output) : base(output)
    {
    }

    class TestObj
    {
        public Berufserfahrungen[] Berufserfahrungen { get; set; }
        public Tag[] Tags { get; set; }
    }
    class Berufserfahrungen
    {
        public string Tätigkeitsbeschreibung { get; set; }
    }
    class Tag
    {
        public string Tagname { get; set; }
    }

    public class Index_TESTIndexOff : AbstractIndexCreationTask
    {
        public class Result
        {
            public string KandidatenId { get; set; }
            public object[] Highlight { get; set; }
        }
            
        public override string IndexName => "TESTIndexOff";

        public override IndexDefinition CreateIndexDefinition()
        {
            return new IndexDefinition
            {
                Maps =
                {
                    @"docs.TestObjs.Select(b => new {
   KandidatenId = Id(b),
    Highlight = new object[] {
        b.Name,
        b.Vorname,
         b.Tags.Select(e14 => e14.Tagname),
        b.Berufserfahrungen.Select(e16 => new {
            Tätigkeitsbeschreibung = e16.Tätigkeitsbeschreibung
        })
       
    }
})"
                },
                Reduce = @"results.Where(result => result.KandidatenId != null).GroupBy(result => result.KandidatenId).Select(g => new {
    g = g,
    b = DynamicEnumerable.FirstOrDefault(g)
}).Select(this1 => new {
    KandidatenId = this1.b.KandidatenId,
    Highlight = new object[] {
        this1.g.Select(e12 => e12.Highlight)
    }
   
})",
                Fields =
                {
                    { "Highlight", new IndexFieldOptions
                    {
                        Indexing = FieldIndexing.Search,
                        TermVector = FieldTermVector.WithPositionsAndOffsets } } ,
                    { "__all_fields", new IndexFieldOptions
                    {
                        Storage = FieldStorage.Yes } }
                }
            };
        }
    }

    [Fact]
    public async Task HighlightOfTermStartingWithCommaShouldWork()
    {
        using (var store = GetDocumentStore())
        {
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new TestObj
                {
                    Berufserfahrungen = new Berufserfahrungen[]
                    {
                        new Berufserfahrungen
                        {
                            Tätigkeitsbeschreibung = ", aa"
                        }
                    },
                    Tags = new Tag[]
                    {
                        new Tag{Tagname = "IT"}
                    }
                });
                await session.SaveChangesAsync();
            }

            await new Index_TESTIndexOff().ExecuteAsync(store);

            using (var session = store.OpenAsyncSession())
            {
                var query = session.Advanced.AsyncDocumentQuery<Index_TESTIndexOff.Result, Index_TESTIndexOff>()
                    .WaitForNonStaleResults()
                    .Highlight("Highlight", 18, 1, out Highlightings titleHighlighting)
                    .Search("Highlight", "aa");
                    

             
                _ = await query
                    .SingleAsync();

            }
        }
    }
}

