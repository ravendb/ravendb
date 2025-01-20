using System;
using System.Collections.Generic;
using Corax;
using Corax.Indexing;
using Corax.Mappings;
using Corax.Querying;
using FastTests.Voron;
using Sparrow;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Corax;

public class RavenDB_23631(ITestOutputHelper output) : StorageTest(output)
{
    [RavenFact(RavenTestCategory.Querying | RavenTestCategory.Corax)]
    public void MultiTermMatchDoesNotReturnDuplicatesWhenPerformingAndWith()
    {
        using var mapping = IndexFieldsMappingBuilder.CreateForWriter(false)
            .AddBinding(0, "id()")
            .AddBinding(1, "name")
            .Build();
        
        using (var writer = new IndexWriter(Env, mapping, SupportedFeatures.All))
        {
            for (int i = 0; i < 1000; i++)
            {
                using (var builder = writer.Index($"id/{i}"))
                {
                    builder.Write(0, Encodings.Utf8.GetBytes($"id/{i}"));
                    builder.IncrementList();
                    builder.Write(1, Encodings.Utf8.GetBytes("name/0"));
                    builder.Write(1, Encodings.Utf8.GetBytes("name/1"));
                    builder.DecrementList();
                    builder.EndWriting();
                }
            }
            
            writer.Commit();
        }

        using (var searcher = new IndexSearcher(Env, mapping))
        {
            var @in = searcher.InQuery("id()", ["id/0", "id/10"]);
            var mtm = searcher.ExistsQuery(mapping.GetByFieldId(1).Metadata);

            var resultMatch = searcher.And(@in, mtm);
            Span<long> ids = stackalloc long[16];
            var read = resultMatch.Fill(ids);
            Assert.Distinct(ids[..read].ToArray());
            Assert.Equal(2, read);
            var nothingLeft = resultMatch.Fill(ids) == 0;
            Assert.True(nothingLeft);
        }
    }
}
