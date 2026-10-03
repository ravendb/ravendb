using System.Text;
using Corax;
using Corax.Mappings;
using FastTests.Voron;
using Sparrow.Server;
using Sparrow.Threading;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;
using IndexSearcher = Corax.Querying.IndexSearcher;
using IndexWriter = Corax.Indexing.IndexWriter;

namespace FastTests.Corax
{
    // A facet with a where clause switches to the per-document scan when the query matched fewer documents than the
    // facet field has terms. The count it compares against is the number of terms the per-term path iterates: one per
    // distinct value and one for null, not one per entry holding a null.
    public class NumberOfDistinctTermsInField : StorageTest
    {
        public NumberOfDistinctTermsInField(ITestOutputHelper output) : base(output)
        {
        }

        private const int IdIndex = 0;
        private const int ColorIndex = 1;

        [RavenFact(RavenTestCategory.Corax)]
        public void CountsNullAsOneTermHoweverManyEntriesHoldIt()
        {
            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            using var builder = IndexFieldsMappingBuilder.CreateForWriter(false)
                .AddBinding(IdIndex, "Id")
                .AddBinding(ColorIndex, "Color");
            using var fields = builder.Build();

            var colors = new[] { "red", "green", "blue" };
            const int entriesWithNull = 50;

            using (var writer = new IndexWriter(Env, fields, SupportedFeatures.All))
            {
                for (var i = 0; i < colors.Length + entriesWithNull; i++)
                {
                    using var entry = writer.Index($"items/{i}");
                    entry.Write(IdIndex, Encoding.UTF8.GetBytes($"items/{i}"));
                    if (i < colors.Length)
                        entry.Write(ColorIndex, Encoding.UTF8.GetBytes(colors[i]));
                    else
                        entry.WriteNull(ColorIndex, null);
                    entry.EndWriting();
                }

                writer.Commit();
            }

            using (var searcher = new IndexSearcher(Env, fields))
            {
                var color = fields.GetByFieldId(ColorIndex).Metadata;

                Assert.Equal(colors.Length + 1, searcher.GetNumberOfDistinctTermsInField(color));

                // the same number of terms the per-term facet path visits
                using (searcher.TextualAggregation(color).AggregateByTerms(out var terms, out _))
                    Assert.Equal(colors.Length + 1, terms.Count);
            }
        }
    }
}
