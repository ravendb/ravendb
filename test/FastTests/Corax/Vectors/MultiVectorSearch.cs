using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using Corax;
using Corax.Indexing;
using Corax.Mappings;
using Corax.Querying;
using Corax.Utils;
using FastTests.Voron;
using Nito.Disposables;
using Raven.Server.Documents.Indexes.VectorSearch;
using Sparrow;
using Sparrow.Server;
using Sparrow.Server.Utils;
using Sparrow.Threading;
using Tests.Infrastructure;
using Voron.Data.Graphs;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Corax.Vectors;

public class MultiVectorSearch(ITestOutputHelper output) : StorageTest(output)
{
    private static readonly float[][] Vectors = [[1f, 1f], [1f, 1.2f], [-1f, -1f], [-1.2f, -1.2f]];

    [RavenFact(RavenTestCategory.Vector | RavenTestCategory.Querying)]
    public void CanSearchMultipleVectors()
    {
        using var _ = GetMappings(out var bsc, out var mapping, out var getVector);
        using (var writer = new IndexWriter(Env, mapping, SupportedFeatures.All))
        {
            var id = 1;
            foreach (var vector in Vectors)
            {
                var idLocal = $"vectors/{id++}";
                using (var entry = writer.Index(idLocal))
                {
                    entry.Write(0, Encodings.Utf8.GetBytes(idLocal));
                    entry.WriteVector(1, "Vector", MemoryMarshal.Cast<float, byte>(vector));
                    entry.EndWriting();
                }
            }

            writer.Commit();
        }

        using (var indexSearcher = new IndexSearcher(Env, mapping))
        {
            var metadata = mapping.GetByFieldId(1).Metadata;
            Span<long> ids = stackalloc long[16];

            var querySingle = indexSearcher.VectorSearch(metadata, getVector(Vectors[0]), 0.75f, 16, false, true, null, scanningThreshold: 0);

            var read = querySingle.Fill(ids);
            Assert.Equal(2, read);

            querySingle = indexSearcher.VectorSearch(metadata, getVector(Vectors[2]), 0.75f, 16, false, true, null, scanningThreshold: 0);
            read = querySingle.Fill(ids);
            Assert.Equal(2, read);

            var combinedQuery = indexSearcher.MultiVectorSearch(metadata, new[] { getVector(Vectors[0]), getVector(Vectors[2]) }, 0.75f, 16, false, true, null, 0);
            read = combinedQuery.Fill(ids);
            Assert.Equal(4, read);
        }

        using (var indexSearcher = new IndexSearcher(Env, mapping))
        {
            var metadata = mapping.GetByFieldId(1).Metadata;
            Span<long> ids = stackalloc long[1];

            var querySingle = indexSearcher.VectorSearch(metadata, getVector(Vectors[0]), 0.75f, 16, false, true, null, scanningThreshold: 0);

            List<long> idsReturned = new();
            var read = querySingle.Fill(ids);
            Assert.Equal(1, read);
            idsReturned.Add(ids[0]);

            querySingle.Fill(ids);
            Assert.Equal(1, read);
            idsReturned.Add(ids[0]);
            Assert.Distinct(idsReturned);


            querySingle = indexSearcher.VectorSearch(metadata, getVector(Vectors[2]), 0.75f, 16, false, true, null, scanningThreshold: 0);
            read = querySingle.Fill(ids);
            Assert.Equal(1, read);
            idsReturned.Add(ids[0]);
            read = querySingle.Fill(ids);
            Assert.Equal(1, read);
            idsReturned.Add(ids[0]);
            Assert.Distinct(idsReturned);
            idsReturned.Clear();

            var combinedQuery = indexSearcher.MultiVectorSearch(metadata, new[] { getVector(Vectors[0]), getVector(Vectors[2]) }, 0.75f, 16, false, true, null, 0);

            //first
            read = combinedQuery.Fill(ids);
            Assert.Equal(1, read);
            idsReturned.Add(ids[0]);

            //second
            read = combinedQuery.Fill(ids);
            Assert.Equal(1, read);
            idsReturned.Add(ids[0]);

            //third
            read = combinedQuery.Fill(ids);
            Assert.Equal(1, read);
            idsReturned.Add(ids[0]);

            //fourth
            read = combinedQuery.Fill(ids);
            Assert.Equal(1, read);
            idsReturned.Add(ids[0]);

            read = combinedQuery.Fill(ids);
            Assert.Equal(0, read);
            Assert.Distinct(idsReturned);
        }
    }

    [RavenFact(RavenTestCategory.Vector | RavenTestCategory.Querying)]
    public void FillWhenNotUsedInSingleQueryWillReturnMatchesSortedByIds()
    {
        using var _ = GetMappings(out var bsc, out var mapping, out var getVector);

        float[][] vectors = [[1f, 1.8f], [1.2f, 1.2f], [0.3f, 0.7f]];
        DocumentEntryId[] sourceIds = [DocumentEntryId.Invalid, DocumentEntryId.Invalid, DocumentEntryId.Invalid];

        using (var indexWriter = new IndexWriter(Env, mapping, SupportedFeatures.All))
        {
            using (var entry = indexWriter.Index("vectors/1"))
            {
                entry.Write(0, "vectors/1"u8);
                entry.WriteVector(1, "Vector", MemoryMarshal.Cast<float, byte>(vectors[0]));
                entry.EndWriting();
                sourceIds[0] = entry.EntryId;
            }

            using (var entry = indexWriter.Index("vectors/2"))
            {
                entry.Write(0, "vectors/2"u8);
                entry.WriteVector(1, "Vector", MemoryMarshal.Cast<float, byte>(vectors[1]));
                entry.EndWriting();
                sourceIds[1] = entry.EntryId;
            }

            using (var entry = indexWriter.Index("vectors/3"))
            {
                entry.Write(0, "vectors/3"u8);
                entry.WriteVector(1, "Vector", MemoryMarshal.Cast<float, byte>(vectors[2]));
                entry.EndWriting();
                sourceIds[2] = entry.EntryId;
            }

            indexWriter.Commit();
        }

        using (var indexSearcher = new IndexSearcher(Env, mapping))
        {
            var metadata = mapping.GetByFieldId(1).Metadata.ChangeScoringMode(true);
            Span<long> ids = stackalloc long[16];
            Span<float> scores = stackalloc float[16];
            
            var query = indexSearcher.MultiVectorSearch(metadata, new[] { getVector(vectors[1]), getVector([-1f, -1f]) }, 0.75f, 16, false, false, filterQuery: null, scanningThreshold: 0);
            
            var read = query.Fill(ids);
            Assert.Equal(3, read);
            Assert.Equal(new long[] { 1L, 2L, 3L }, ids[..3]);
            query.Score(ids[..3], scores[..3], 1f);
            Assert.Equal(scores[0], 0.96f, 0.01); // doc 1
            Assert.Equal(scores[1], 1f, 0.01); // doc 2
            Assert.Equal(scores[2], 0.92f, 0.01); // doc3


            query = indexSearcher.MultiVectorSearch(metadata, new[] { getVector(vectors[1]), getVector([-1f, -1f]) }, 0.75f, 16, false, true, filterQuery: null, scanningThreshold: 0);
            read = query.Fill(ids);
            Assert.Equal(3, read);
            Assert.Equal((long)sourceIds[1], ids[0]);
            query.Score(ids[..3], scores[..3], 1f);
            Assert.True(scores[0] > scores[1]);
            Assert.True(scores[1] > scores[2]);
        }

        //AND WITH
        using (var indexSearcher = new IndexSearcher(Env, mapping))
        {
            var metadata = mapping.GetByFieldId(1).Metadata.ChangeScoringMode(true);

            var query = indexSearcher.MultiVectorSearch(metadata, new[] { getVector(vectors[1]), getVector([-1f, -1f]) }, 0.75f, 16, false, false, filterQuery: null, scanningThreshold: 0);

            Span<long> toAndWith = stackalloc long[16];
            toAndWith[0] = 1L;
            toAndWith[1] = 3L;

            var resultOfAndWith = query.AndWith(toAndWith, 2);
            Assert.Equal(2, resultOfAndWith);
            Assert.Equal(1L, toAndWith[0]);
            Assert.Equal(3L, toAndWith[1]);
        }
    }

    [RavenFact(RavenTestCategory.Vector | RavenTestCategory.Querying)]
    public void SortAndMinOnDuplicatesShouldWork()
    {
        Span<long> values = [2, 1, 2, 3];
        Span<float> itemsAssociated = [0.5f, 0.4f, 0.5f, 0.7f];

        var length = Sorting.SortAndMinOnDuplicates(values, itemsAssociated);

        Assert.Equal(3, length);

        Assert.Equal(1, values[0]);
        Assert.Equal(2, values[1]);
        Assert.Equal(3, values[2]);

        Assert.Equal(0.4f, itemsAssociated[0]);
        Assert.Equal(0.5f, itemsAssociated[1]);
        Assert.Equal(0.7f, itemsAssociated[2]);
    }

    [RavenFact(RavenTestCategory.Vector | RavenTestCategory.Querying)]
    public void MultiVectorSearchShouldSumDuplicates()
    {
        float[][] vectors = [[1f, 1f], [2f, 0f], [-1f, 3f]];

        using var _ = GetMappings(out var _, out var mapping, out var getVector);
        using (var writer = new IndexWriter(Env, mapping, SupportedFeatures.All))
        {
            var id = 1;
            foreach (var vector in vectors)
            {
                var idLocal = $"vectors/{id++}";
                using (var entry = writer.Index(idLocal))
                {
                    entry.Write(0, Encodings.Utf8.GetBytes(idLocal));
                    entry.WriteVector(1, "Vector", MemoryMarshal.Cast<float, byte>(vector));
                    entry.EndWriting();
                }
            }

            writer.Commit();
        }

        using (var indexSearcher = new IndexSearcher(Env, mapping))
        {
            var metadata = mapping.GetByFieldId(1).Metadata;
            Span<long> ids = stackalloc long[16];

            var combinedQuery = indexSearcher.MultiVectorSearch(metadata, new[] { getVector(vectors[0]), getVector(vectors[2]) }, 0.0f, 16, false, true, filterQuery: null, scanningThreshold: 0);
            var read = combinedQuery.Fill(ids);
            Assert.Equal(3, read);
            Assert.Equal(ids[2], 2);
        }
    }

    private static IDisposable GetMappings(out ByteStringContext bsc, out IndexFieldsMapping mapping, out Func<float[], VectorValue> getVector)
    {
        var bscLocal = new ByteStringContext(SharedMultipleUseFlag.None);
        var mappingLocal = IndexFieldsMappingBuilder
            .CreateForWriter(false)
            .AddBinding(0, "id()")
            .AddBinding(1, "Vector", vectorOptions: new VectorOptions() { NumberOfEdges = 4, NumberOfCandidates = 16, VectorEmbeddingType = VectorEmbeddingType.Single })
            .Build();

        bsc = bscLocal;
        mapping = mappingLocal;
        getVector = vector =>
        {
            var scope = bscLocal.Allocate(sizeof(float) * vector.Length, out Memory<byte> vec);
            MemoryMarshal.Cast<float, byte>(vector).CopyTo(vec.Span);
            return GenerateEmbeddings.FromArray(bscLocal, scope, vec, Raven.Client.Documents.Indexes.Vector.VectorOptions.Default, sizeof(float) * vector.Length);
        };

        return Disposable.Create(() =>
        {
            bscLocal.Dispose();
            mappingLocal.Dispose();
        });
    }
}
