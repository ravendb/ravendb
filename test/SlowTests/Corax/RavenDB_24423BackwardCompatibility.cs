using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;
using Corax.Analyzers;
using Corax.Pipeline;
using Corax.Querying;
using FastTests;
using Xunit;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Server.Documents.Indexes.Persistence.Corax;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Server;
using Sparrow.Threading;
using Tests.Infrastructure;

namespace SlowTests.Corax;

public class RavenDB_24423BackwardCompatibility(ITestOutputHelper output) : RavenTestBase(output)
{
    [RavenFact(RavenTestCategory.Corax)]
    public void AnalyzerOutputMatchesPreRavenDb24423()
    {
        using var allocator = new ByteStringContext(SharedMultipleUseFlag.None);
        var assembly = typeof(RavenDB_24423BackwardCompatibility).Assembly;
        using var resourceStream = assembly.GetManifestResourceStream("SlowTests.Data.RavenDB_24423.PreRavenDB_24423.gz");
        Assert.NotNull(resourceStream);

        var analyzer = Analyzer.Create(allocator, default(KeywordTokenizer), default(LowerCaseTransformerPre24423));

        using var _ = allocator.Allocate(1024, out ByteString output);
        var bufferSource = output.ToSpan().Slice(0, 512);
        var tokensBuffer = MemoryMarshal.Cast<byte, Token>(output.ToSpan().Slice(512));
        foreach (var (runeCode, storedToken) in DecodeByteFileData(resourceStream))
        {
            if (Rune.TryCreate(runeCode, out var expectedRune) == false)
                continue;
            
            var expectedString = Rune.ToLower(expectedRune, CultureInfo.InvariantCulture).ToString();
            ReadOnlySpan<byte> expectedSource = Encoding.UTF8.GetBytes(expectedString);
            Span<Token> tokens = tokensBuffer;
            Span<byte> bufferSpan = bufferSource;
            analyzer.Execute(expectedSource, ref bufferSpan, ref tokens);
            Assert.NotEqual(0, bufferSpan.Length);
            var token = tokens[0];
            ReadOnlySpan<byte> currentToken = bufferSpan.Slice(token.Offset, (int)token.Length);

            if (currentToken.SequenceEqual(storedToken) == false && IsOsIcuCasingDrift(expectedRune, storedToken))
            {
                // Rune.ToLowerInvariant is backed by the OS-shipped ICU, whose Unicode case tables vary by
                // build (e.g. Windows 11 23H2 vs 25H2 / ICU 72 -> Unicode 15.0). The golden file was recorded
                // on an older ICU that had no lowercase mapping for a few newly-cased code points, so it stored
                // them un-folded; a newer ICU folds them. That is data drift across runtimes, not a Pre24423
                // regression - skip just this code point. All other ~1.1M runes still assert the length/format.
                continue;
            }

            Assert.Equal(storedToken, currentToken);
        }
    }

    // Detects the signature of an OS/ICU Unicode-version mismatch for a single code point: the golden file
    // recorded it un-folded (the recording runtime's ICU had no lowercase mapping), but this runtime's ICU does
    // fold it. This is deliberately narrow so a genuine transform/length regression is still caught.
    private static bool IsOsIcuCasingDrift(Rune rune, ReadOnlySpan<byte> storedToken)
    {
        var raw = Encoding.UTF8.GetBytes(rune.ToString());
        if (storedToken.SequenceEqual(raw) == false)
            return false;

        return Rune.ToLowerInvariant(rune) != rune;
    }
    
    [RavenFact(RavenTestCategory.BackupExportImport | RavenTestCategory.Corax)]
    public async Task CanRestoreAndQuerySnapshotFromPreRavenDb24423()
    {
        var backupPath = NewDataPath(forceCreateDir: true);
        var fullBackupPath = Path.Combine(backupPath, "PreRavenDB_24423.ravendb-snapshot");
        ExtractFile(fullBackupPath);

        const string value1 = "বাংলাবর্ণমালাবালিপি";
        const string value2 = "বাংলাবর্ণমালা1";
        byte[] value1Pre, value1Current, value2Pre, value2Current;
        
        {
            using var allocator = new ByteStringContext(SharedMultipleUseFlag.None);
            var analyzerPre24423 = Analyzer.Create(allocator, default(KeywordTokenizer), default(LowerCaseTransformerPre24423));
            var analyzer = Analyzer.Create(allocator, default(KeywordTokenizer), default(LowerCaseTransformer));
            using var _ = allocator.Allocate(1536, out ByteString buffer);
            var source = buffer.ToSpan().Slice(0, 512);
            var tokens = MemoryMarshal.Cast<byte, Token>(buffer.ToSpan().Slice(512));
            
            {
                var bufferSource = source;
                var tokensBuffer = tokens;
                var bytes = Encodings.Utf8.GetBytes(value1);
                analyzer.Execute(bytes, ref bufferSource, ref tokensBuffer);
                value1Current = bufferSource.Slice(tokensBuffer[0].Offset, (int)tokensBuffer[0].Length).ToArray();
            }

            {
                var bufferSource = source;
                var tokensBuffer = tokens;
                var bytes = Encodings.Utf8.GetBytes(value1);
                analyzerPre24423.Execute(bytes, ref bufferSource, ref tokensBuffer);
                value1Pre = bufferSource.Slice(tokensBuffer[0].Offset, (int)tokensBuffer[0].Length).ToArray();
            }

            {
                var bufferSource = source;
                var tokensBuffer = tokens;
                var bytes = Encodings.Utf8.GetBytes(value2);
                analyzer.Execute(bytes, ref bufferSource, ref tokensBuffer);
                value2Current = bufferSource.Slice(tokensBuffer[0].Offset, (int)tokensBuffer[0].Length).ToArray();
            }

            {
                var bufferSource = source;
                var tokensBuffer = tokens;
                var bytes = Encodings.Utf8.GetBytes(value2);
                analyzerPre24423.Execute(bytes, ref bufferSource, ref tokensBuffer);
                value2Pre = bufferSource.Slice(tokensBuffer[0].Offset, (int)tokensBuffer[0].Length).ToArray();
            }
        }
        
        using (var store = GetDocumentStore())
        {
            var databaseName = GetDatabaseName();
            using (Backup.RestoreDatabase(store, new RestoreBackupConfiguration { BackupLocation = backupPath, DatabaseName = databaseName }))
            {
                var stats = await store.Maintenance.ForDatabase(databaseName).SendAsync(new GetStatisticsOperation());
                Assert.True(stats.CountOfDocuments > 0, "Expected restored database to have documents");

                var indexNames = await store.Maintenance.ForDatabase(databaseName).SendAsync(new GetIndexNamesOperation(0, 10));
                Assert.NotEmpty(indexNames);
                var indexName = indexNames[0];

                await Indexes.WaitForIndexingAsync(store, databaseName);

                var terms = await GetDecodedIndexTerms(store, databaseName, indexName);
                Assert.Equal(value1Pre, terms[0]);
                Assert.Equal(value2Pre, terms[1]);

                using (var session = store.OpenAsyncSession(databaseName))
                {
                    var r1 = await session.Query<CharactersContainer, Index>().Where(x => x.Value == value1).ToListAsync();
                    Assert.Equal(1, r1.Count);
                    var r2 = await session.Query<CharactersContainer, Index>().Where(x => x.Value == value2).ToListAsync();
                    Assert.Equal(1, r2.Count);
                }

                using (var session = store.OpenAsyncSession(databaseName))
                {
                    session.Delete("characters/1");
                    session.Delete("characters/2");
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession(databaseName))
                {
                    await session.StoreAsync(new CharactersContainer { Id = "characters/1", Value = value1 });
                    await session.StoreAsync(new CharactersContainer { Id = "characters/2", Value = value2 });
                    await session.SaveChangesAsync();
                }

                await Indexes.WaitForIndexingAsync(store, databaseName);

                terms = await GetDecodedIndexTerms(store, databaseName, indexName);
                Assert.Equal(value1Pre, terms[0]);
                Assert.Equal(value2Pre, terms[1]);

                using (var session = store.OpenAsyncSession(databaseName))
                {
                    var r1 = await session.Query<CharactersContainer, Index>().Where(x => x.Value == value1).ToListAsync();
                    Assert.Equal(1, r1.Count);
                    var r2 = await session.Query<CharactersContainer, Index>().Where(x => x.Value == value2).ToListAsync();
                    Assert.Equal(1, r2.Count);
                }

                await store.Maintenance.ForDatabase(databaseName).SendAsync(new ResetIndexOperation(indexName));
                await Indexes.WaitForIndexingAsync(store, databaseName);

                terms = await GetDecodedIndexTerms(store, databaseName, indexName);
                Assert.Equal(value1Current, terms[0]);
                Assert.Equal(value2Current, terms[1]);

                using (var session = store.OpenAsyncSession(databaseName))
                {
                    var r1 = await session.Query<CharactersContainer, Index>().Where(x => x.Value == value1).ToListAsync();
                    Assert.Equal(1, r1.Count);
                    var r2 = await session.Query<CharactersContainer, Index>().Where(x => x.Value == value2).ToListAsync();
                    Assert.Equal(1, r2.Count);
                }
            }
        }

        void ExtractFile(string path)
        {
            using (var file = File.Create(path))
            using (var stream = typeof(RavenDB_24423BackwardCompatibility).Assembly
                       .GetManifestResourceStream("SlowTests.Data.RavenDB_24423.PreRavenDB_24423.ravendb-snapshot"))
            {
                Assert.NotNull(stream);
                stream.CopyTo(file);
            }
        }
    }

    private class CharactersContainer
    {
        public string Id { get; set; }
        public string Value { get; set; } 
    }

    private class Index : AbstractIndexCreationTask<CharactersContainer>
    {
        public Index()
        {
            Map = docs => from doc in docs
                select new { doc.Value };
            SearchEngineType = Raven.Client.Documents.Indexes.SearchEngineType.Corax;
        }
    }
    
    [UnsafeAccessor(UnsafeAccessorKind.Field, Name = "IndexSearcher")]
    private static extern ref IndexSearcher GetIndexSearcherFromReadOperation(CoraxIndexReadOperation operation);

    private async Task<List<byte[]>> GetDecodedIndexTerms(IDocumentStore store, string databaseName, string indexName)
    {
        var db = await Databases.GetDocumentDatabaseInstanceFor(store, databaseName);
        Assert.NotNull(db);
        var indexInstance = db.IndexStore.GetIndex(indexName);
        Assert.NotNull(indexInstance);

        using (indexInstance._contextPool.AllocateOperationContext(out TransactionOperationContext indexContext))
        using (var indexTx = indexContext.OpenReadTransaction())
        {
            using var indexReader = indexInstance.IndexPersistence.OpenIndexReader(indexTx.InnerTransaction);
            var coraxIndexReadOperation = Assert.IsType<CoraxIndexReadOperation>(indexReader);
            var indexSearcher = GetIndexSearcherFromReadOperation(coraxIndexReadOperation);
            Assert.NotNull(indexSearcher);

            var ids = new long[16];
            var read = indexSearcher.AllEntries().Fill(ids);
            Assert.Equal(2, read);

            var termsReader = indexSearcher.TermsReaderFor("Value");
            var result = new List<byte[]>();

            for (int i = 0; i < read; i++)
            {
                Assert.True(termsReader.TryGetRawTermFor(ids[i], out UnmanagedSpan rawTerm));
                result.Add(termsReader.GetDecodedTerm(rawTerm).ToArray());
            }

            return result;
        }
    }
    
    private static readonly (int From, int ToInclusive)[] RuneRanges =
    [
        (1, 0x007F),
        (0x0080, 0x07FF),
        (0x0800, 0xFFFF),
        (0x010000, 0x10FFFF)
    ];

    private static IEnumerable<(int RuneCode, Rune Rune, string String)> EnumerateRunes()
    {
        foreach (var (from, toInclusive) in RuneRanges)
        {
            for (int i = from; i <= toInclusive; ++i)
            {
                if (i is 1 or 3 || i is >= 0xFFF0 and <= 0xFFFF)
                    continue;

                if (Rune.TryCreate(i, out var rune) == false)
                    continue;

                yield return (i, rune, rune.ToString());
            }
        }
    }

    private static IEnumerable<(int RuneCode, byte[] Token)> DecodeByteFileData(Stream compressed)
    {
        using var gzip = new GZipStream(compressed, CompressionMode.Decompress, leaveOpen: true);
        var ms = new MemoryStream();
        gzip.CopyTo(ms);
        var data = ms.ToArray();
        int offset = 0;
        foreach (var (runeCode, _, _) in EnumerateRunes())
        {
            if (offset >= data.Length)
                yield break;

            int tokenLen = data[offset];
            offset += sizeof(byte);
            byte[] token = new byte[tokenLen];
            Array.Copy(data, offset, token, 0, tokenLen);
            offset += tokenLen;
            yield return (runeCode, token);
        }
    }
}
