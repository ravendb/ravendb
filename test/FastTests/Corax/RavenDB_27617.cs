using System;
using System.Linq;
using Corax.Analyzers;
using Corax.Pipeline;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Indexes;
using Sparrow.Server;
using Sparrow.Threading;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Corax
{
    public class RavenDB_27617(ITestOutputHelper output) : RavenTestBase(output)
    {
        // U+023A lower-cases to U+2C65, so 2 UTF-8 bytes become 3 (U+023E -> U+2C66 behaves the same).
        // Corax sizes the lower-casing destination to the source length, so every such letter makes the
        // writer drift one byte ahead of the reader. The bulk ASCII copy in
        // ScalarTransformers.ToLowercaseSingleToken writes 8 bytes at a time with no destination bounds
        // check, so once enough drift has accumulated it writes past the end of the rented buffer.
        //
        // Above 1 MB, Analyzer.BufferPool (ArrayPool<byte>.Create()) hands back an array of exactly the
        // requested length instead of a padded power-of-two, so the overrun lands outside the allocation
        // rather than in slack. On the unfixed build this crashes the host process
        // (AccessViolationException / "Internal CLR error" from corrupted heap). The defect is the one
        // tracked as RavenDB-27617.
        [RavenFact(RavenTestCategory.Corax)]
        public void LowercasingAValueWhoseUtf8GrowsMustNotWritePastTheBuffer()
        {
            const int growingLetters = 600_000; // U+023A: each pushes the writer one byte ahead of the reader
            const int asciiTail = 800_000;      // plain ASCII, run through the unchecked bulk copy at the end

            ReadOnlySpan<byte> source = BuildValue(growingLetters, asciiTail);

            // The exact-size rented buffer (and therefore the overrun) only happens above 1 MB.
            Assert.True(source.Length > 1024 * 1024);

            using var context = new ByteStringContext(SharedMultipleUseFlag.None);
            var analyzer = Analyzer.CreateLowercaseAnalyzer(context);

            // Size the buffers the way the indexing path (IndexEntryBuilder.AnalyzeTerm) does.
            analyzer.GetOutputBuffersSize(source.Length, out var outputSize, out var tokenSize);
            var outputBuffer = new byte[outputSize];
            var tokenBuffer = new Token[tokenSize];

            Span<byte> outputSpan = outputBuffer;
            Span<Token> tokenSpan = tokenBuffer;

            // On the unfixed build this call writes ~600 KB past the end of an internal buffer.
            analyzer.Execute(source, ref outputSpan, ref tokenSpan);

            // Reached only once the overrun is fixed: the value must still lower-case correctly.
            Assert.Equal(1, tokenSpan.Length);
            ReadOnlySpan<byte> term = outputSpan.Slice(tokenSpan[0].Offset, (int)tokenSpan[0].Length);
            Assert.True(term.SequenceEqual(ExpectedLowercase(growingLetters, asciiTail)));
        }

        // End-to-end: store documents with such a value, then make RavenDB build a Corax auto-index that
        // lower-cases every value. On the unfixed build this kills the server while it indexes the first
        // document; once fixed, indexing completes with no errors and every document is indexed.
        [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Indexes)]
        public void IndexingAValueWhoseUtf8GrowsMustNotCrashTheServer()
        {
            const int documents = 4;
            const int growingLetters = 600_000;
            const int asciiTail = 700_000; // must exceed growingLetters so the drifted writer reaches the bulk copy

            using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));

            using (var session = store.OpenSession())
            {
                for (var i = 0; i < documents; i++)
                    session.Store(new Item { Id = $"items/{i}", Text = BuildText(i, growingLetters, asciiTail) });
                session.SaveChanges();
            }

            // Referencing Text in a query builds the auto-index on it, which lower-cases every stored value.
            using (var session = store.OpenSession())
                session.Advanced.RawQuery<Item>("from Items where Text = $t").AddParameter("t", "x").ToList();

            Indexes.WaitForIndexing(store);

            var errors = store.Maintenance.Send(new GetIndexErrorsOperation());
            Assert.Empty(errors.SelectMany(e => e.Errors));

            var dbStats = store.Maintenance.Send(new GetStatisticsOperation());
            var autoIndex = dbStats.Indexes.Single(i => i.Name.StartsWith("Auto/Items", StringComparison.OrdinalIgnoreCase));
            var indexStats = store.Maintenance.Send(new GetIndexStatisticsOperation(autoIndex.Name));
            Assert.Equal(documents, indexStats.EntriesCount);
        }

        // Regression guard (reported on the PR by BorShiik): the 1.5x reserve tightens the Analyzer.Execute
        // guard, so AnalyzeTerm must grow its buffer against the analyzer output size, not the raw value
        // length. A static-index ASCII value in the (8192 / 1.5, 8192] window fit the buffer yet failed the
        // tightened guard with "Buffer is too small" before the caller was fixed.
        [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Indexes)]
        public void AsciiValueBetween5462And8192BytesIsIndexed()
        {
            using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));
            using (var session = store.OpenSession())
            {
                session.Store(new Item { Text = new string('a', 6000) }, "items/1");
                session.SaveChanges();
            }

            store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition { Name = "Items/Text", Maps = { "from i in docs.Items select new { i.Text }" } }));
            Indexes.WaitForIndexing(store, allowErrors: true);
            RavenTestHelper.AssertNoIndexErrors(store);
        }

        // Regression guard (reported on the PR by BorShiik): dictionary training analyzes samples into a
        // fixed buffer. The 1.5x reserve made a non-ASCII sample over ~170 UTF-8 bytes fail the guard, so
        // training silently fell back to the default dictionary. It must analyze without errors instead.
        [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Indexes)]
        public void DictionaryTrainingHandlesNonAsciiStrings()
        {
            using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));
            using (var session = store.OpenSession())
            {
                session.Store(new Item { Text = new string('Ж', 128) }, "items/1"); // U+0416 CYRILLIC Ж, 2 UTF-8 bytes
                session.SaveChanges();
            }

            store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition { Name = "Items/TextJs", Maps = { "map('Items', i => ({ Text: i.Text + '' }))" } }));
            Indexes.WaitForIndexing(store);
            var perf = store.Maintenance.Send(new GetIndexPerformanceStatisticsOperation(new[] { "Items/TextJs" }));
            Assert.All(perf.SelectMany(x => x.Performance), p => Assert.Equal(0, p.FailedCount));
        }

        private static byte[] BuildValue(int growingLetters, int asciiTail)
        {
            var value = new byte[growingLetters * 2 + asciiTail];
            var i = 0;
            for (var n = 0; n < growingLetters; n++)
            {
                value[i++] = 0xC8; // U+023A, two UTF-8 bytes
                value[i++] = 0xBA;
            }
            for (var n = 0; n < asciiTail; n++)
                value[i++] = (byte)'a';
            return value;
        }

        private static byte[] ExpectedLowercase(int growingLetters, int asciiTail)
        {
            var value = new byte[growingLetters * 3 + asciiTail];
            var i = 0;
            for (var n = 0; n < growingLetters; n++)
            {
                value[i++] = 0xE2; // U+2C65, three UTF-8 bytes
                value[i++] = 0xB1;
                value[i++] = 0xA5;
            }
            for (var n = 0; n < asciiTail; n++)
                value[i++] = (byte)'a';
            return value;
        }

        private static string BuildText(int number, int growingLetters, int asciiTail)
        {
            return $"{number:D6} " + new string('Ⱥ', growingLetters) + new string('a', asciiTail);
        }

        private sealed class Item
        {
            public string Id { get; set; }
            public string Text { get; set; }
        }
    }
}
