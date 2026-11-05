using System;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Lucene.Net.Analysis;
using Lucene.Net.Analysis.Tokenattributes;
using Raven.Server.Documents.Indexes.Persistence.Lucene;
using Tests.Infrastructure;
using Xunit;
using LuceneAnalyzer = Lucene.Net.Analysis.Analyzer;
using Token = Corax.Pipeline.Token;

namespace FastTests.Corax;

public class LuceneAnalyzerAdapterTests(ITestOutputHelper output) : NoDisposalNeeded(output)
{
    /// <summary>A stream of the given terms. An empty term is a token whose start and end offsets are the same.</summary>
    private sealed class FixedTokensStream : TokenStream
    {
        private readonly string[] _terms;
        private readonly ITermAttribute _term;
        private readonly IOffsetAttribute _offset;
        private int _next;
        private int _position;

        public FixedTokensStream(string[] terms)
        {
            _terms = terms;
            _term = AddAttribute<ITermAttribute>();
            _offset = AddAttribute<IOffsetAttribute>();
        }

        public override bool IncrementToken()
        {
            if (_next >= _terms.Length)
                return false;

            var term = _terms[_next++];
            ClearAttributes();
            _term.SetTermBuffer(term);
            _offset.SetOffset(_position, _position + term.Length);
            _position += term.Length + 1;
            return true;
        }

        public override void Reset()
        {
            _next = 0;
            _position = 0;
        }

        protected override void Dispose(bool disposing)
        {
        }
    }

    private sealed class FixedTokensAnalyzer(params string[] terms) : LuceneAnalyzer
    {
        public override TokenStream TokenStream(string fieldName, TextReader reader) => new FixedTokensStream(terms);
    }

    private static string[] Analyze(LuceneAnalyzer analyzer, bool forQuerying)
    {
        var adapter = LuceneAnalyzerAdapter.Create(analyzer, forQuerying);
        var source = Encoding.UTF8.GetBytes("ignored by the analyzer above");
        var outBuffer = new byte[1024];
        var tokenBuffer = new Token[64];

        // an analyzer that gets stuck must fail the test, not hang the run
        var work = Task.Run(() =>
        {
            Span<byte> output = outBuffer;
            Span<Token> tokens = tokenBuffer;
            adapter.Execute(source, ref output, ref tokens);
            var result = new string[tokens.Length];
            for (var i = 0; i < tokens.Length; i++)
                result[i] = Encoding.UTF8.GetString(output.Slice(tokens[i].Offset, (int)tokens[i].Length));

            return result;
        });

        Assert.True(work.Wait(TimeSpan.FromSeconds(10)), "The adapter did not return: it is stuck on an empty token");
        return work.Result;
    }

    [RavenTheory(RavenTestCategory.Corax)]
    [InlineData(false)]
    [InlineData(true)]
    public void AnEmptyTokenIsSkippedAndTheTokensAroundItAreKept(bool forQuerying)
    {
        Assert.Equal(new[] { "ab", "cd" }, Analyze(new FixedTokensAnalyzer("ab", "", "cd"), forQuerying));
    }

    [RavenTheory(RavenTestCategory.Corax)]
    [InlineData(false)]
    [InlineData(true)]
    public void AnEmptyFirstOrLastTokenIsSkipped(bool forQuerying)
    {
        Assert.Equal(new[] { "ab" }, Analyze(new FixedTokensAnalyzer("", "ab", ""), forQuerying));
        Assert.Empty(Analyze(new FixedTokensAnalyzer("", ""), forQuerying));
    }

    [RavenTheory(RavenTestCategory.Corax)]
    [InlineData(false)]
    [InlineData(true)]
    public void NormalTokensAreReturnedInOrder(bool forQuerying)
    {
        Assert.Equal(new[] { "one", "two", "three" }, Analyze(new FixedTokensAnalyzer("one", "two", "three"), forQuerying));
    }
}
