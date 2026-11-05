using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using Corax.Pipeline;
using Lucene.Net.Analysis;
using Lucene.Net.Analysis.Standard;
using Raven.Server.Documents.Indexes.Persistence.Lucene;
using Tests.Infrastructure;
using Xunit;
using LuceneAnalyzer = Lucene.Net.Analysis.Analyzer;
using LuceneVersion = Lucene.Net.Util.Version;
using Token = Corax.Pipeline.Token;

namespace FastTests.Corax;

/// <summary>
/// Cost of a custom Lucene analyzer that only overrides TokenStream and builds a new chain for every value.
/// Opt-in: RAVEN_ANALYZER_BENCH=1. Writes to $RAVEN_ANALYZER_BENCH_OUT (default /tmp/analyzer-bench.log).
/// </summary>
public class AnalyzerReuseBenchmark(ITestOutputHelper output) : NoDisposalNeeded(output)
{
    /// <summary>Only TokenStream is overridden, so nothing is reused.</summary>
    private sealed class GlobalCharacterAnalyzer : LuceneAnalyzer
    {
        public override TokenStream TokenStream(string fieldName, TextReader reader)
        {
            var tokenizer = new StandardTokenizer(LuceneVersion.LUCENE_29, reader);
            var standardFilter = new StandardFilter(tokenizer);
            var lowerCaseFilter = new LowerCaseFilter(standardFilter);
            return new ASCIIFoldingFilter(lowerCaseFilter);
        }
    }

    /// <summary>The same chain, built once per thread; the tokenizer is pointed at the next value.</summary>
    private sealed class ReusableGlobalCharacterAnalyzer : LuceneAnalyzer
    {
        private sealed class SavedStreams
        {
            public Tokenizer Source;
            public TokenStream Result;
        }

        public override TokenStream TokenStream(string fieldName, TextReader reader)
        {
            var tokenizer = new StandardTokenizer(LuceneVersion.LUCENE_29, reader);
            return new ASCIIFoldingFilter(new LowerCaseFilter(new StandardFilter(tokenizer)));
        }

        public override TokenStream ReusableTokenStream(string fieldName, TextReader reader)
        {
            if (PreviousTokenStream is not SavedStreams saved)
            {
                var tokenizer = new StandardTokenizer(LuceneVersion.LUCENE_29, reader);
                saved = new SavedStreams { Source = tokenizer, Result = new ASCIIFoldingFilter(new LowerCaseFilter(new StandardFilter(tokenizer))) };
                PreviousTokenStream = saved;
            }
            else
            {
                saved.Source.Reset(reader);
            }

            return saved.Result;
        }
    }

    private static readonly string[] Titles =
    [
        "Senior Software Engineer (m/f/x)",
        "Medewerker Klantenservice - Café Ménage Düsseldorf",
        "Verpleegkundige Ziekenhuis Zwolle, Amsterdam & Rotterdam",
        "Account Manager Façade Systems",
        "Warehouse Operator Nachtdienst",
        "Projectleider Bouw en Infra – Zürich / Genève",
        "Chauffeur CE (Dutch/Deutsch/English)",
        "Teamleider Logistiek & Distributie, 32-40 uur",
    ];

    private static string Run(LuceneAnalyzer analyzer, byte[][] sources, int rounds, out long allocatedBytes, out string tokensDigest)
    {
        var adapter = LuceneAnalyzerAdapter.Create(analyzer, forQuerying: false);
        var outBuffer = new byte[16 * 1024];
        var tokenBuffer = new Token[1024];
        var digest = new StringBuilder();

        // warm-up, and the tokens for the correctness check
        foreach (var source in sources)
        {
            Span<byte> output = outBuffer;
            Span<Token> tokens = tokenBuffer;
            adapter.Execute(source, ref output, ref tokens);
            foreach (var token in tokens)
                digest.Append(Encoding.UTF8.GetString(output.Slice(token.Offset, (int)token.Length))).Append('|');
            digest.Append('\n');
        }

        for (var i = 0; i < 2000; i++)
        {
            Span<byte> output = outBuffer;
            Span<Token> tokens = tokenBuffer;
            adapter.Execute(sources[i % sources.Length], ref output, ref tokens);
        }

        GC.Collect();
        var before = GC.GetAllocatedBytesForCurrentThread();
        var watch = Stopwatch.StartNew();
        var total = 0;
        for (var r = 0; r < rounds; r++)
        {
            Span<byte> output = outBuffer;
            Span<Token> tokens = tokenBuffer;
            adapter.Execute(sources[r % sources.Length], ref output, ref tokens);
            total += tokens.Length;
        }

        watch.Stop();
        allocatedBytes = GC.GetAllocatedBytesForCurrentThread() - before;
        tokensDigest = digest.ToString();
        return $"{watch.Elapsed.TotalMilliseconds * 1_000_000 / rounds:F0} ns/value, {allocatedBytes / (double)rounds:F0} B/value, {total / (double)rounds:F1} tokens/value";
    }

    [RavenFact(RavenTestCategory.Corax)]
    public void CostOfNotReusingTheTokenStream()
    {
        if (Environment.GetEnvironmentVariable("RAVEN_ANALYZER_BENCH") != "1")
            return;

        var sources = Titles.Select(Encoding.UTF8.GetBytes).ToArray();
        const int rounds = 400_000;
        var results = new StringBuilder();
        string first = null;

        foreach (var (name, make) in new (string, Func<LuceneAnalyzer>)[]
                 {
                     ("not reused (as written in the dump)", () => new GlobalCharacterAnalyzer()),
                     ("reused (ReusableTokenStream)", () => new ReusableGlobalCharacterAnalyzer()),
                     ("not reused (again)", () => new GlobalCharacterAnalyzer()),
                     ("reused (again)", () => new ReusableGlobalCharacterAnalyzer()),
                 })
        {
            var line = Run(make(), sources, rounds, out _, out var digest);
            results.AppendLine($"{name}: {line}");
            first ??= digest;
            Assert.Equal(first, digest); // the same tokens
        }

        File.AppendAllText(Environment.GetEnvironmentVariable("RAVEN_ANALYZER_BENCH_OUT") ?? "/tmp/analyzer-bench.log", results.ToString());
        output.WriteLine(results.ToString());
    }
}
