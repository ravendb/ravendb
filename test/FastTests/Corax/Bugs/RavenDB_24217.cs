using System;
using System.Linq;
using Corax;
using Corax.Indexing;
using Corax.Mappings;
using Corax.Querying;
using FastTests.Voron;
using FastTests.Voron.FixedSize;
using Sparrow;
using Tests.Infrastructure;
using Voron;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Corax.Bugs;

public class RavenDB_24217(ITestOutputHelper output) : StorageTest(output)
{
    [RavenFact(RavenTestCategory.Corax)]
    public void CanDeleteAndStillUpdateFanout()
    {
        using var mapping = GetMapping();
        long[] entriesIds = [-1, -1, -1, -1];

        var entries = new Entry[] { new("doc/0", "0"), new("doc/0", "1"), new("doc/1", "2"), new("doc/1", "3"), };
        using (var writer = new IndexWriter(Env, mapping, SupportedFeatures.All))
        {
            for (int i = 0; i < entries.Length; ++i)
            {
                var entry = entries[i];
                using (var builder = writer.Index(entry.PrimaryKeyBytes))
                {
                    builder.Write(PrimaryKey, entry.PrimaryKeyBytes);
                    builder.Write(SecondaryKey, entry.SecondaryKeyBytes);
                    builder.EndWriting();
                    entriesIds[i] = builder.EntryId;
                }
            }

            writer.Commit();
        }

        using (var writer = new IndexWriter(Env, mapping, SupportedFeatures.All))
        {
            writer.TryDeleteEntry("doc/1"u8);
            writer.TryDeleteEntry("doc/0"u8);

            for (int i = 3; i >= 0; --i)
            {
                var entry = entries[i];
                using (var builder = writer.Index(entry.PrimaryKeyBytes))
                {
                    builder.Write(PrimaryKey, entry.PrimaryKeyBytes);
                    builder.Write(SecondaryKey, entry.SecondaryKeyBytes);
                    builder.EndWriting();
                    entry.EntryId = builder.EntryId;
                }
            }

            //Should not be reused
            Assert.DoesNotContain(entries[0].EntryId, entriesIds);
            Assert.DoesNotContain(entries[1].EntryId, entriesIds);
            Assert.DoesNotContain(entries[2].EntryId, entriesIds);
            Assert.DoesNotContain(entries[3].EntryId, entriesIds);
            Assert.Distinct(entries.Select(x => x.EntryId));

            writer.Commit();

            Assert.True(writer.ForTestingPurposes().ValidateIdTreeToEntries(out _, out _));
        }

        using (var searcher = new IndexSearcher(Env, mapping))
        {
            var rootPages = searcher.GetIndexedFieldNamesByRootPage();
            Page p = default;

            foreach (var entry in entries)
            {
                var entryTermsReader = searcher.GetEntryTermsReader(entry.EntryId, ref p);
                entryTermsReader.Reset();

                while (entryTermsReader.MoveNext())
                {
                    Assert.True(rootPages.TryGetValue(entryTermsReader.FieldRootPage, out Slice fieldName));

                    switch (fieldName.ToString())
                    {
                        case PrimaryKeyName:
                            Assert.Equal(entry.PrimaryKeyBytes, entryTermsReader.Current.Decoded());
                            break;
                        case SecondaryKeyName:
                            Assert.Equal(entry.SecondaryKeyBytes, entryTermsReader.Current.Decoded());
                            break;
                        default:
                            throw new ArgumentOutOfRangeException("Unknown field name: " + fieldName);
                    }
                }
            }
        }
    }


    [RavenFact(RavenTestCategory.Corax)]
    public void CanDeleteAndStillUpdate()
    {
        using var mapping = GetMapping();

        using (var writer = new IndexWriter(Env, mapping, SupportedFeatures.All))
        {
            using (var builder = writer.Index("doc/0"u8))
            {
                builder.Write(PrimaryKey, "doc/0"u8);
                builder.Write(SecondaryKey, "0"u8);
                builder.EndWriting();
            }

            using (var builder = writer.Index("doc/1"u8))
            {
                builder.Write(PrimaryKey, "doc/1"u8);
                builder.Write(SecondaryKey, "1"u8);
                builder.EndWriting();
            }

            writer.Commit();
        }

        long doc1Id, doc0Id;
        using (var writer = new IndexWriter(Env, mapping, SupportedFeatures.All))
        {
            writer.TryDeleteEntry("doc/1"u8);
            writer.TryDeleteEntry("doc/0"u8);

            using (var builder = writer.Update("doc/1"u8))
            {
                builder.Write(PrimaryKey, "doc/1"u8);
                builder.Write(SecondaryKey, "1"u8);
                builder.EndWriting();

                doc1Id = builder.EntryId;
            }

            using (var builder = writer.Update("doc/0"u8))
            {
                builder.Write(PrimaryKey, "doc/0"u8);
                builder.Write(SecondaryKey, "0"u8);
                builder.EndWriting();

                doc0Id = builder.EntryId;
            }

            writer.Commit();
            
            Assert.True(writer.ForTestingPurposes().ValidateIdTreeToEntries(out _, out _));
        }

        using (var searcher = new IndexSearcher(Env, mapping))
        {
            var rootPages = searcher.GetIndexedFieldNamesByRootPage();
            Page p = default;

            var entryTermsReader = searcher.GetEntryTermsReader(doc0Id, ref p);
            entryTermsReader.Reset();
            while (entryTermsReader.MoveNext())
            {
                Assert.True(rootPages.TryGetValue(entryTermsReader.FieldRootPage, out Slice fieldName));

                switch (fieldName.ToString())
                {
                    case PrimaryKeyName:
                        Assert.Equal("doc/0"u8, entryTermsReader.Current.Decoded());
                        break;
                    case SecondaryKeyName:
                        Assert.Equal("0"u8, entryTermsReader.Current.Decoded());
                        break;
                    default:
                        throw new ArgumentOutOfRangeException("Unknown field name: " + fieldName);
                }
            }

            entryTermsReader = searcher.GetEntryTermsReader(doc1Id, ref p);
            entryTermsReader.Reset();
            while (entryTermsReader.MoveNext())
            {
                Assert.True(rootPages.TryGetValue(entryTermsReader.FieldRootPage, out Slice fieldName));

                switch (fieldName.ToString())
                {
                    case PrimaryKeyName:
                        Assert.Equal("doc/1"u8, entryTermsReader.Current.Decoded());
                        break;
                    case SecondaryKeyName:
                        Assert.Equal("1"u8, entryTermsReader.Current.Decoded());
                        break;
                    default:
                        throw new ArgumentOutOfRangeException("Unknown field name: " + fieldName);
                }
            }
        }


        using (var writer = new IndexWriter(Env, mapping, SupportedFeatures.All))
        {
            var testing = writer.ForTestingPurposes();
            var result = testing.ValidateIdTreeToEntries(out var numberOfEntriesLocation, out var numberOfEntriesCompactTreeId);
            Assert.Equal(numberOfEntriesCompactTreeId, numberOfEntriesLocation);
            Assert.True(result);
        }
    }

    [RavenTheory(RavenTestCategory.Corax)]
    [InlineDataWithRandomSeed]
    [InlineDataWithRandomSeed]
    [InlineDataWithRandomSeed]
    public void CanDeleteAndStillUpdateFuzzy(int seed)
    {
        using var mapping = GetMapping();
        var random = new Random(seed);
        var docCount = random.Next(4, 512);

        var entries = new Entry[docCount];
        using (var writer = new IndexWriter(Env, mapping, SupportedFeatures.All))
        {
            for (int docId = 0; docId < docCount; ++docId)
            {
                var newEntry = entries[docId] = new Entry($"doc/{docId}", docId.ToString());


                using (var builder = writer.Index(newEntry.PrimaryKeyBytes))
                {
                    builder.Write(PrimaryKey, newEntry.PrimaryKeyBytes);
                    builder.Write(SecondaryKey, Encodings.Utf8.GetBytes(docId.ToString()));
                    builder.EndWriting();
                    newEntry.EntryId = builder.EntryId;
                }
            }

            writer.Commit();
        }


        using (var writer = new IndexWriter(Env, mapping, SupportedFeatures.All))
        {
            // We want to test in removals in random order.
            var toRemove = Enumerable.Range(0, docCount).ToArray();
            random.Shuffle(toRemove.AsSpan());
            foreach (var entryPosToRemove in toRemove)
            {
                var entry = entries[entryPosToRemove];
                writer.TryDeleteEntry(entry.PrimaryKeyBytes);
            }

            var toAdd = Enumerable.Range(0, docCount).ToArray();
            random.Shuffle(toAdd);

            foreach (var entryIdToAdd in toAdd)
            {
                var entry = entries[entryIdToAdd];
                entry.Reindex();

                using (var builder = writer.Update(entry.PrimaryKeyBytes))
                {
                    builder.Write(PrimaryKey, entry.PrimaryKeyBytes);
                    builder.Write(SecondaryKey, entry.SecondaryKeyBytes);
                    builder.EndWriting();
                }
            }

            writer.Commit();
            
            Assert.True(writer.ForTestingPurposes().ValidateIdTreeToEntries(out _, out _));
        }


        using (var searcher = new IndexSearcher(Env, mapping))
        {
            var rootPages = searcher.GetIndexedFieldNamesByRootPage();
            Page p = default;

            foreach (var entry in entries)
            {
                var entryTermsReader = searcher.GetEntryTermsReader(entry.EntryId, ref p);
                entryTermsReader.Reset();

                while (entryTermsReader.MoveNext())
                {
                    Assert.True(rootPages.TryGetValue(entryTermsReader.FieldRootPage, out Slice fieldName));

                    switch (fieldName.ToString())
                    {
                        case PrimaryKeyName:
                            Assert.Equal(entry.PrimaryKeyBytes, entryTermsReader.Current.Decoded());
                            break;
                        case SecondaryKeyName:
                            Assert.Equal(entry.SecondaryKeyBytes, entryTermsReader.Current.Decoded());
                            break;
                        default:
                            throw new ArgumentOutOfRangeException("Unknown field name: " + fieldName);
                    }
                }
            }
        }
    }

    private const int PrimaryKey = 0;
    private const string PrimaryKeyName = "id()";
    private const int SecondaryKey = 1;
    private const string SecondaryKeyName = "name";

    private static IndexFieldsMapping GetMapping() => IndexFieldsMappingBuilder.CreateForWriter(false)
        .AddBinding(PrimaryKey, PrimaryKeyName)
        .AddBinding(SecondaryKey, SecondaryKeyName)
        .Build();

    private class Entry(string primaryKey, string secondaryKey)
    {
        private string _secondaryKey = secondaryKey;
        public long EntryId { get; set; }

        public ReadOnlySpan<byte> PrimaryKeyBytes => Encodings.Utf8.GetBytes(primaryKey);
        public ReadOnlySpan<byte> SecondaryKeyBytes => Encodings.Utf8.GetBytes(_secondaryKey);

        public void Reindex() => _secondaryKey = $"{SecondaryKey}/Updated";
    }
}
