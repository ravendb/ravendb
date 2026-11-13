using System;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using Lucene.Net.Analysis;
using Lucene.Net.Analysis.Tokenattributes;
using Raven.Server.Documents.Indexes.Persistence.Lucene;
using Tests.Infrastructure;
using Xunit;
using LuceneAnalyzer = Lucene.Net.Analysis.Analyzer;
using Token = Corax.Pipeline.Token;

namespace FastTests.Issues
{
    public class RavenDB_27942 : NoDisposalNeeded
    {
        public RavenDB_27942(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Corax)]
        [InlineData(false)]
        [InlineData(true)]
        public void AdapterSkipsEmptyTokensAndKeepsTheOthers(bool forQuerying)
        {
            Assert.Equal(["ab", "cd"], Analyze(new FixedTokensAnalyzer("", "ab", "", "cd", ""), forQuerying));
            Assert.Empty(Analyze(new FixedTokensAnalyzer("", ""), forQuerying));
        }

        private static string[] Analyze(LuceneAnalyzer analyzer, bool forQuerying)
        {
            var adapter = LuceneAnalyzerAdapter.Create(analyzer, forQuerying);
            var source = Encoding.UTF8.GetBytes("ignored");

            // the adapter used to spin forever on an empty token, fail instead of hanging the run
            var work = Task.Run(() =>
            {
                Span<byte> output = new byte[1024];
                Span<Token> tokens = new Token[64];
                adapter.Execute(source, ref output, ref tokens);

                var result = new string[tokens.Length];
                for (var i = 0; i < tokens.Length; i++)
                    result[i] = Encoding.UTF8.GetString(output.Slice(tokens[i].Offset, (int)tokens[i].Length));
                return result;
            });

            Assert.True(work.Wait(TimeSpan.FromSeconds(10)), "the adapter did not return");
            return work.Result;
        }

        private sealed class FixedTokensAnalyzer(params string[] terms) : LuceneAnalyzer
        {
            public override TokenStream TokenStream(string fieldName, TextReader reader) => new FixedTokensStream(terms);
        }

        // an empty term is emitted as a token with the same start and end offsets
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
    }
}
