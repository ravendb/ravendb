//-----------------------------------------------------------------------
// <copyright file="CollationAnalyzer.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System.Globalization;
using System.IO;
using Lucene.Net.Analysis;

namespace Raven.Server.Documents.Indexes.Persistence.Lucene.Collation
{
    public class CollationAnalyzer : Analyzer
    {
        private CultureInfo _cultureInfo;

        public CollationAnalyzer(CultureInfo cultureInfo)
        {
            Init(cultureInfo);
        }

        protected void Init(CultureInfo ci)
        {
            _cultureInfo = ci;
        }

        protected CollationAnalyzer()
        {

        }

        public override TokenStream TokenStream(string fieldName, TextReader reader)
        {
            TokenStream result = new KeywordTokenizer(reader);
            return new CollationKeyFilter(result, _cultureInfo);
        }

        private class SavedStreams
        {
            public Tokenizer Source;
            public TokenStream Result;
        }

        public override TokenStream ReusableTokenStream(string fieldName, TextReader reader)
        {
            var streams = (SavedStreams)PreviousTokenStream;
            if (streams == null)
            {
                streams = new SavedStreams();
                streams.Source = new KeywordTokenizer(reader);
                streams.Result = new CollationKeyFilter(streams.Source, _cultureInfo);
                PreviousTokenStream = streams;
            }
            else
            {
                streams.Source.Reset(reader);
            }
            return streams.Result;
        }
    }
}
