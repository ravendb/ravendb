using System;
using Corax;
using Corax.Indexing;
using Corax.Mappings;
using Corax.Querying;
using FastTests.Voron;
using Tests.Infrastructure;
using Voron;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Corax.Bugs;

public class RavenDB_23606(ITestOutputHelper output) : StorageTest(output)
{
    [RavenFact(RavenTestCategory.Indexes | RavenTestCategory.Querying)]
    public void CanUpdateAndPersistNumericalValueInEntriesToTerms()
    {
        using var mapping = IndexFieldsMappingBuilder.CreateForWriter(isDynamic: false)
            .AddBinding(0, "id")
            .AddBinding(1, "month")
            .AddBinding(2, "year")
            .Build();
        
        using (var writer = new IndexWriter(Env, mapping, SupportedFeatures.All))
        {
            using (var entryBuilder = writer.Update("id/1"u8))
            {
                entryBuilder.Write(0, "id/1"u8);
                entryBuilder.Write(1, "1"u8, 1L, 1D);
                entryBuilder.Write(2, "2024"u8, 2024L, 2024D);
                entryBuilder.EndWriting();
            }
            
            writer.Commit();
        }
        
        using (var writer = new IndexWriter(Env, mapping, SupportedFeatures.All))
        {
            using (var entryBuilder = writer.Update("id/2"u8))
            {
                entryBuilder.Write(0, "id/2"u8);
                entryBuilder.Write(1, "1"u8, 1L, 1D);
                entryBuilder.Write(2, "2024"u8, 2024L, 2024D);
                entryBuilder.EndWriting();
            }
            
            writer.Commit();
        }
        
        using (var writer = new IndexWriter(Env, mapping, SupportedFeatures.All))
        {
            using (var entryBuilder = writer.Update("id/3"u8))
            {
                entryBuilder.Write(0, "id/3"u8);
                entryBuilder.Write(1, "1"u8, 1L, 1D);
                entryBuilder.Write(2, "2024"u8, 2024L, 2024D);
                entryBuilder.EndWriting();
            }
            
            using (var entryBuilder = writer.Update("id/1"u8))
            {
                entryBuilder.Write(0, "id/1"u8);
                entryBuilder.Write(1, "1"u8, 1L, 1D);
                entryBuilder.Write(2, "2024"u8, 2024L, 2024D);
                entryBuilder.EndWriting();
            }
            
            writer.Commit();
        }

        // Tree for longs
        using (var indexSearcher = new IndexSearcher(Env, mapping))
        {
            Span<long> ids = new long[16];
            var read = indexSearcher.AllEntries().Fill(ids);
            Assert.Equal(3, read);
            ids = ids[..read];
            var terms = new long[read];
            var lookup = indexSearcher.EntriesToTermsReader(mapping.GetByFieldId(2).FieldNameLong);
            lookup.GetFor(ids, terms, long.MinValue);
            Assert.Equal(2024L, terms[0]);
            Assert.Equal(2024L, terms[1]);
            Assert.Equal(2024L, terms[2]);
        }
        
        
        // Tree for doubles
        using (var indexSearcher = new IndexSearcher(Env, mapping))
        {
            Span<long> ids = new long[16];
            var read = indexSearcher.AllEntries().Fill(ids);
            Assert.Equal(3, read);
            ids = ids[..read];
            var terms = new long[read];
            var lookup = indexSearcher.EntriesToTermsReader(mapping.GetByFieldId(2).FieldNameDouble);
            lookup.GetFor(ids, terms, BitConverter.DoubleToInt64Bits(double.MinValue));
            Assert.Equal(2024D, BitConverter.Int64BitsToDouble(terms[0]));
            Assert.Equal(2024D, BitConverter.Int64BitsToDouble(terms[1]));
            Assert.Equal(2024D, BitConverter.Int64BitsToDouble(terms[2]));
        }
    }
}
