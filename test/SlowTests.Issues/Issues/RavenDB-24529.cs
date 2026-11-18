using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using Corax;
using Corax.Analyzers;
using Corax.Indexing;
using Corax.Mappings;
using Corax.Querying;
using FastTests.Voron;
using Sparrow;
using Sparrow.Server;
using Sparrow.Threading;
using Tests.Infrastructure;
using Voron;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_24529(ITestOutputHelper output) : StorageTest(output)
{
    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Indexes)]
    public void CanMixNumericalAndTextualValuesWithExactlyTheSameTextualRepresentationInList()
    {
        using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
        var analyzer = Analyzer.CreateLowercaseAnalyzer(bsc);
        using var mapping = IndexFieldsMappingBuilder.CreateForWriter(isDynamic: false)
            .AddBinding(0, "id()", analyzer)
            .AddBinding(1, "Int", analyzer)
            .Build();

        long doc1Id, doc2Id, doc3Id;
        using (var writer = new IndexWriter(Env, mapping, SupportedFeatures.All))
        {
            using (var builder = writer.Index("doc1"))
            {
                doc1Id = (long)builder.EntryId;
                builder.Write(0, "id()", Encodings.Utf8.GetBytes("doc1"));
                builder.IncrementList();
                builder.Write(1, "Int", Encodings.Utf8.GetBytes("1"));
                builder.Write(1, "Int", Encodings.Utf8.GetBytes("1"), 1, 1.0);
                builder.DecrementList();
                builder.EndWriting();
            }

            using (var builder = writer.Index("doc2"))
            {
                doc2Id = (long)builder.EntryId;
                builder.Write(0, "id()", Encodings.Utf8.GetBytes("doc2"));
                builder.Write(1, "Int", Encodings.Utf8.GetBytes("1"), 1, 1.0);
                builder.EndWriting();
            }

            using (var builder = writer.Index("doc3"))
            {
                doc3Id = (long)builder.EntryId;
                builder.Write(0, "id()", Encodings.Utf8.GetBytes("doc3"));
                builder.Write(1, "Int", Encodings.Utf8.GetBytes("1"), 1, 1.0);
                builder.EndWriting();
            }

            writer.Commit();
        }

        using (var indexSearcher = new IndexSearcher(Env, mapping))
        {
            Span<long> ids = stackalloc long[16];

            Assert.Equal(3, indexSearcher.TermQuery(mapping.GetByFieldId(1).Metadata, "1").Fill(ids));
            Assert.Equal(3, indexSearcher.TermQuery(mapping.GetByFieldId(1).Metadata, 1L).Fill(ids));
            Page p = default;
            Assert.True(indexSearcher.TryGetRootPageByFieldName(mapping.GetByFieldId(1).Metadata.FieldName, out var rootPageForInt));
            var reader = indexSearcher.GetEntryTermsReader(doc1Id, ref p);
            Assert.True(reader.FindNext(rootPageForInt));
            Assert.True(reader.HasNumeric);

            reader = indexSearcher.GetEntryTermsReader(doc2Id, ref p);
            Assert.True(reader.FindNext(rootPageForInt));
            Assert.True(reader.HasNumeric);

            reader = indexSearcher.GetEntryTermsReader(doc3Id, ref p);
            Assert.True(reader.FindNext(rootPageForInt));
            Assert.True(reader.HasNumeric);
        }

        using (var writer = new IndexWriter(Env, mapping, SupportedFeatures.All))
        {
            using (var builder = writer.Update("doc1"u8))
            {
                builder.Write(0, "id()", Encodings.Utf8.GetBytes("doc1"));
                builder.Write(1, "Int", Encodings.Utf8.GetBytes("1"));
                builder.EndWriting();
            }

            writer.Commit();
        }

        using (var indexSearcher = new IndexSearcher(Env, mapping))
        {
            Span<long> ids = stackalloc long[16];

            Assert.Equal(3, indexSearcher.TermQuery(mapping.GetByFieldId(1).Metadata, "1").Fill(ids));
            Assert.Equal(2, indexSearcher.TermQuery(mapping.GetByFieldId(1).Metadata, 1L).Fill(ids));
            Page p = default;
            Assert.True(indexSearcher.TryGetRootPageByFieldName(mapping.GetByFieldId(1).Metadata.FieldName, out var rootPageForInt));
            var reader = indexSearcher.GetEntryTermsReader(doc1Id, ref p);
            Assert.True(reader.FindNext(rootPageForInt));
            Assert.False(reader.HasNumeric);

            reader = indexSearcher.GetEntryTermsReader(doc2Id, ref p);
            Assert.True(reader.FindNext(rootPageForInt));
            Assert.True(reader.HasNumeric);

            reader = indexSearcher.GetEntryTermsReader(doc3Id, ref p);
            Assert.True(reader.FindNext(rootPageForInt));
            Assert.True(reader.HasNumeric);
        }
    }

    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Indexes)]
    public void CanMixNumericalAndTextualValuesWithExactlyTheSameTextualRepresentation()
    {
        using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
        var analyzer = Analyzer.CreateLowercaseAnalyzer(bsc);
        using var mapping = IndexFieldsMappingBuilder.CreateForWriter(isDynamic: false)
            .AddBinding(0, "id()", analyzer)
            .AddBinding(1, "Int", analyzer)
            .Build();

        long doc1Id, doc2Id, doc3Id;
        using (var writer = new IndexWriter(Env, mapping, SupportedFeatures.All))
        {
            using (var builder = writer.Index("doc1"))
            {
                doc1Id = (long)builder.EntryId;
                builder.Write(0, "id()", Encodings.Utf8.GetBytes("doc1"));
                builder.Write(1, "Int", Encodings.Utf8.GetBytes("1"));
                builder.EndWriting();
            }

            using (var builder = writer.Index("doc2"))
            {
                doc2Id = (long)builder.EntryId;
                builder.Write(0, "id()", Encodings.Utf8.GetBytes("doc2"));
                builder.Write(1, "Int", Encodings.Utf8.GetBytes("1"), 1, 1.0);
                builder.EndWriting();
            }

            using (var builder = writer.Index("doc3"))
            {
                doc3Id = (long)builder.EntryId;
                builder.Write(0, "id()", Encodings.Utf8.GetBytes("doc3"));
                builder.Write(1, "Int", Encodings.Utf8.GetBytes("1"), 1, 1.0);
                builder.EndWriting();
            }

            writer.Commit();
        }

        using (var indexSearcher = new IndexSearcher(Env, mapping))
        {
            Span<long> ids = stackalloc long[16];

            Assert.Equal(3, indexSearcher.TermQuery(mapping.GetByFieldId(1).Metadata, "1").Fill(ids));
            Assert.Equal(2, indexSearcher.TermQuery(mapping.GetByFieldId(1).Metadata, 1L).Fill(ids));
            Page p = default;
            Assert.True(indexSearcher.TryGetRootPageByFieldName(mapping.GetByFieldId(1).Metadata.FieldName, out var rootPageForInt));
            var reader = indexSearcher.GetEntryTermsReader(doc1Id, ref p);
            Assert.True(reader.FindNext(rootPageForInt));
            Assert.False(reader.HasNumeric);

            reader = indexSearcher.GetEntryTermsReader(doc2Id, ref p);
            Assert.True(reader.FindNext(rootPageForInt));
            Assert.True(reader.HasNumeric);

            reader = indexSearcher.GetEntryTermsReader(doc3Id, ref p);
            Assert.True(reader.FindNext(rootPageForInt));
            Assert.True(reader.HasNumeric);
        }

        using (var writer = new IndexWriter(Env, mapping, SupportedFeatures.All))
        {
            Assert.True(writer.TryDeleteEntry("doc1"));
            writer.Commit();
        }

        using (var indexSearcher = new IndexSearcher(Env, mapping))
        {
            Span<long> ids = stackalloc long[16];

            Assert.Equal(2, indexSearcher.TermQuery(mapping.GetByFieldId(1).Metadata, "1").Fill(ids));
            Assert.DoesNotContain(doc1Id, ids[..2].ToArray());

            Assert.Equal(2, indexSearcher.TermQuery(mapping.GetByFieldId(1).Metadata, 1L).Fill(ids));
            Assert.DoesNotContain(doc1Id, ids[..2].ToArray());

            Page p = default;
            Assert.True(indexSearcher.TryGetRootPageByFieldName(mapping.GetByFieldId(1).Metadata.FieldName, out var rootPageForInt));

            var reader = indexSearcher.GetEntryTermsReader(doc2Id, ref p);
            Assert.True(reader.FindNext(rootPageForInt));
            Assert.True(reader.HasNumeric);

            reader = indexSearcher.GetEntryTermsReader(doc3Id, ref p);
            Assert.True(reader.FindNext(rootPageForInt));
            Assert.True(reader.HasNumeric);
        }
    }


    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Indexes)]
    public void CanMixNumericalAndTextualValuesWithExactlyTheSameTextualRepresentationBigScenario()
    {
        using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
        var analyzer = Analyzer.CreateLowercaseAnalyzer(bsc);
        using var mapping = IndexFieldsMappingBuilder.CreateForWriter(isDynamic: false)
            .AddBinding(0, "id()", analyzer)
            .AddBinding(1, "Int", analyzer)
            .Build();

        var commands = ReadAllLines($"RavenDB_24529.RavenDB_24529.replay").ToList();
        IndexWriter writer = null;
        IndexWriter.IndexEntryBuilder builder = null;
        List<long> intNumericalField = new();
        List<long> intTextualField = new();
        try
        {
            foreach (var command in commands)
            {
                writer ??= new IndexWriter(Env, mapping, SupportedFeatures.All);
                if (command.StartsWith("COMMIT"))
                {
                    using (writer)
                    {
                        EndWritingPreviousEntry();
                        writer.Commit();
                    }

                    writer = null;
                    builder = null;
                    AssertPostingListsToActualRuntimeValues();
                    continue;
                }

                var commandParts = command.Split('|');
                if (command.StartsWith("UPDATE"))
                {
                    EndWritingPreviousEntry();
                    builder = writer!.Update(Encodings.Utf8.GetBytes(commandParts[1]));

                    UpdateDocument();
                    continue;
                }

                if (command.StartsWith("INDEX"))
                {
                    EndWritingPreviousEntry();
                    builder = writer!.Index(Encodings.Utf8.GetBytes(commandParts[1]));
                    continue;
                }

                if (command.StartsWith("Text"))
                {
                    var fieldId = int.Parse(commandParts[1]);
                    var fieldPath = commandParts[2];
                    var value = commandParts[3];
                    builder.Write(fieldId, fieldPath, Encodings.Utf8.GetBytes(value));
                    InsertTextualValue(fieldPath, fieldId);
                    continue;
                }

                if (command.StartsWith("Numerical"))
                {
                    var fieldId = int.Parse(commandParts[1]);
                    var fieldPath = commandParts[2];
                    var value = commandParts[3];
                    var valueLong = long.Parse(commandParts[3]);
                    var valueDouble = double.Parse(commandParts[4]);
                    builder.Write(fieldId, fieldPath, Encodings.Utf8.GetBytes(value), valueLong, valueDouble);
                    InsertNumericalValue(fieldPath, fieldId);
                    continue;
                }

                throw new InvalidOperationException("Invalid operation from the datasource!");
            }
        }
        finally
        {
            writer?.Dispose();
        }

        Assert.Null(writer);
        Assert.Null(builder);

        void InsertTextualValue(string fieldPath, int fieldId)
        {
            if (fieldPath != "Int" || fieldId != 1)
                return;
            intTextualField.Add((long)builder.EntryId);
        }

        void InsertNumericalValue(string fieldPath, int fieldId)
        {
            if (fieldPath != "Int" || fieldId != 1)
                return;
            intNumericalField.Add((long)builder.EntryId);
            intTextualField.Add((long)builder.EntryId);
        }

        void UpdateDocument()
        {
            intTextualField.Remove((long)builder.EntryId);
            intNumericalField.Remove((long)builder.EntryId);
        }

        void EndWritingPreviousEntry()
        {
            if (builder != null)
            {
                builder.EndWriting();
                builder.Dispose();
            }
        }

        void AssertPostingListsToActualRuntimeValues()
        {
            using var indexSearcher = new IndexSearcher(Env, mapping);
            Span<long> ids = new long[16 * 1024];

            {
                //Textual assertion
                var textualTermMatch = indexSearcher.TermQuery(mapping.GetByFieldId(1).Metadata, "1");
                var count = 0;
                while (textualTermMatch.Fill(ids[count..]) is var read and > 0)
                    count += read;

                var runtimeData = CollectionsMarshal.AsSpan(intTextualField);
                runtimeData.Sort();

                Assert.True(count == runtimeData.Length);
                Assert.True(ids[..count].SequenceEqual(runtimeData));
            }

            {
                //Long assertion
                var numericalTermMatch = indexSearcher.TermQuery(mapping.GetByFieldId(1).Metadata, 1L);
                var count = 0;
                while (numericalTermMatch.Fill(ids[count..]) is var read and > 0)
                    count += read;

                var runtimeData = CollectionsMarshal.AsSpan(intNumericalField);
                runtimeData.Sort();


                var notPersisted = runtimeData.ToArray().Except(ids[..count].ToArray()).ToList();
                Assert.Empty(notPersisted);

                var shouldNotBePersisted = ids[..count].ToArray().Except(runtimeData.ToArray()).ToList();
                Assert.Empty(shouldNotBePersisted);

                Assert.Equal(runtimeData.Length, count);
                Assert.True(ids[..count].SequenceEqual(runtimeData));
            }

            {
                var total = 0;
                var allEntries = indexSearcher.AllEntries();
                while (allEntries.Fill(ids[total..]) is var read and > 0)
                    total += read;

                Assert.True(indexSearcher.TryGetRootPageByFieldName(mapping.GetByFieldId(1).Metadata.FieldName, out var rootPageForInt));
                Page p = default;
                for (int idX = 0; idX < total; ++idX)
                {
                    var currentDocument = ids[idX];
                    var reader = indexSearcher.GetEntryTermsReader(currentDocument, ref p);
                    Assert.True(reader.FindNext(rootPageForInt));
                    var hasNumericalValue = intNumericalField.Contains(currentDocument);
                    Assert.Equal(hasNumericalValue, reader.HasNumeric);
                }
            }
        }
    }

    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Indexes)]
    public void ReadSmallPostingListAndUpdate()
    {
        using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
        var analyzer = Analyzer.CreateLowercaseAnalyzer(bsc);
        using var mapping = IndexFieldsMappingBuilder.CreateForWriter(isDynamic: false)
            .AddBinding(0, "id()", analyzer)
            .AddBinding(1, "Int", analyzer)
            .Build();

        var allNumerical = new List<long>();

        using (var indexWriter = new IndexWriter(Env, mapping, SupportedFeatures.All))
        {
            using (var builder = indexWriter.Index("doc1"))
            {
                builder.Write(0, "id()", Encodings.Utf8.GetBytes("doc1"));
                builder.Write(1, "Int", Encodings.Utf8.GetBytes("1"));
                builder.EndWriting();
            }

            for (int i = 2; i < 266; ++i)
            {
                using (var builder = indexWriter.Index($"doc{i}"))
                {
                    builder.Write(0, "id()", Encodings.Utf8.GetBytes($"doc{i}"));
                    builder.Write(1, "Int", Encodings.Utf8.GetBytes("1"), 1L, 1D);
                    allNumerical.Add((long)builder.EntryId);
                    builder.EndWriting();
                }
            }

            indexWriter.Commit();
        }

        using (var indexSearcher = new IndexSearcher(Env, mapping))
        {
            Span<long> ids = new long[300];
            var tm = indexSearcher.TermQuery(mapping.GetByFieldId(1).Metadata, 1L);
            int total = 0;
            while (tm.Fill(ids[total..]) is var read and > 0)
                total += read;

            allNumerical.Sort();
            ids[..total].Sort();
            Assert.True(ids[..total].SequenceEqual(CollectionsMarshal.AsSpan(allNumerical)));
        }

        using (var indexWriter = new IndexWriter(Env, mapping, SupportedFeatures.All))
        {
            using (var builder = indexWriter.Update("doc1"u8))
            {
                builder.Write(0, "id()", Encodings.Utf8.GetBytes("doc1"));
                builder.Write(1, "Int", Encodings.Utf8.GetBytes("1"), 1L, 1D);
                allNumerical.Add((long)builder.EntryId);
                builder.EndWriting();
            }

            indexWriter.Commit();
        }

        using (var indexSearcher = new IndexSearcher(Env, mapping))
        {
            Span<long> ids = new long[300];
            var tm = indexSearcher.TermQuery(mapping.GetByFieldId(1).Metadata, 1L);
            int total = 0;
            while (tm.Fill(ids[total..]) is var read and > 0)
                total += read;

            allNumerical.Sort();
            ids[..total].Sort();
            
            Assert.True(ids[..total].SequenceEqual(CollectionsMarshal.AsSpan(allNumerical)));
        }
    }

    private static IEnumerable<string> ReadAllLines(string name)
    {
        using (var stream = typeof(RavenDB_24529).Assembly.GetManifestResourceStream("SlowTests.Data." + name))
        using (var reader = new StreamReader(stream))
        {
            string line;
            while ((line = reader.ReadLine()) != null)
            {
                if (string.IsNullOrEmpty(line))
                    continue;
                yield return line;
            }
        }
    }
}
