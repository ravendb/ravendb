using System;
using System.Globalization;

using Lucene.Net.Analysis;
using Lucene.Net.Analysis.Standard;
using Lucene.Net.Analysis.Tokenattributes;

namespace Raven.Server.Documents.Indexes.Persistence.Lucene.Analyzers
{
    public class RavenStandardFilter : TokenFilter
    {
        public RavenStandardFilter(TokenStream input) : base(input)
        {
            _termAtt = AddAttribute<ITermAttribute>();
            _typeAtt = AddAttribute<ITypeAttribute>();
            _innerInputStream = input;
        }

        private readonly TokenStream _innerInputStream;
        private const string APOSTROPHE_TYPE = "<APOSTROPHE>";
        private const string ACRONYM_TYPE = "<ACRONYM>";

        private readonly ITypeAttribute _typeAtt;
        private readonly ITermAttribute _termAtt;
        public override bool IncrementToken()
        {
            if (!input.IncrementToken())
            {
                return false;
            }
            bool bufferUpdated = true;
            char[] buffer = _termAtt.TermBuffer();
            int bufferLength = _termAtt.TermLength();
            String type = _typeAtt.Type;

            if (type == APOSTROPHE_TYPE && bufferLength >= 2 && buffer[bufferLength - 2] == '\'' && (buffer[bufferLength - 1] == 's' || buffer[bufferLength - 1] == 'S'))
            {
                for (int i = 0; i < bufferLength - 2; i++)
                {
                    buffer[i] = ToLower(buffer[i]);
                }
                // Strip last 2 characters off
                _termAtt.SetTermLength(bufferLength - 2);
            }
            else if (type == ACRONYM_TYPE)
            {
                // remove dots
                int upto = 0;
                for (int i = 0; i < bufferLength; i++)
                {
                    char c = buffer[i];
                    if (c != '.')
                        buffer[upto++] = ToLower(c);
                }
                _termAtt.SetTermLength(upto);
            }
            else
            {
                do
                {
                    //If we consumed a stop word we need to update the buffer and its length.
                    if (!bufferUpdated)
                    {
                        bufferLength = _termAtt.TermLength();
                        buffer = _termAtt.TermBuffer();
                    }

                    for (int i = 0; i < bufferLength; i++)
                    {
                        buffer[i] = ToLower(buffer[i]);
                    }
                    if (!_stopWords.Contains(buffer, 0, bufferLength))
                    {
                        return true;
                    }
                    bufferUpdated = false;
                } while (input.IncrementToken());
                return false;
            }
            return true;
        }

        protected char ToLower(char c)
        {
            int cInt = c;

            if (c < 128 && IsAsciiCasingSameAsInvariant)
            {
                if (65 <= cInt && cInt <= 90)
                    c |= ' ';

                return c;
            }
            return InvariantTextInfo.ToLower(c);

        }

        public bool Reset(System.IO.TextReader reader)
        {
            var input = (StandardTokenizer)_innerInputStream;
            if (input != null)
            {
                input.Reset(reader);
                return true;
            }
            return false;
        }

        private static readonly bool IsAsciiCasingSameAsInvariant = CultureInfo.InvariantCulture.CompareInfo.Compare("abcdefghijklmnopqrstuvwxyz", "ABCDEFGHIJKLMNOPQRSTUVWXYZ", CompareOptions.IgnoreCase) == 0;
        private static readonly TextInfo InvariantTextInfo = CultureInfo.InvariantCulture.TextInfo;
        private readonly CharArraySet _stopWords = new CharArraySet(StopAnalyzer.ENGLISH_STOP_WORDS_SET, false);
    }
}
