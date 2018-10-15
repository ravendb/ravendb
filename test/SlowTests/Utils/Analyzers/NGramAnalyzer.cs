// -----------------------------------------------------------------------
//  <copyright file="NGramAnalyzer.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.IO;
using Lucene.Net.Analysis;
using Lucene.Net.Analysis.Standard;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.Static;
using Version = Lucene.Net.Util.Version;

namespace SlowTests.Utils.Analyzers
{
    [NotForQuerying]
    public class NGramAnalyzer : Analyzer
    {
        private readonly int _minGram;
        private readonly int _maxGram;

        public NGramAnalyzer()
        {
            var scope = CurrentIndexingScope.Current;
            if (scope == null)
                throw new InvalidOperationException("Indexing scope was not initialized.");

            _minGram = scope.Index.Configuration.MinGram;
            _maxGram = scope.Index.Configuration.MaxGram;
        }

        public override TokenStream TokenStream(string fieldName, TextReader reader)
        {
            var tokenizer = new StandardTokenizer(Version.LUCENE_29, reader)
            {
                MaxTokenLength = 255
            };

            TokenStream filter = new StandardFilter(tokenizer);
            filter = new LowerCaseFilter(filter);
            filter = new StopFilter(false, filter, StandardAnalyzer.STOP_WORDS_SET);
            return new NGramTokenFilter(filter, _minGram, _maxGram);
        }
    }
}
