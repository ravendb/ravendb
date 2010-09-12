using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Lucene.Net.Analysis;
using Lucene.Net.Analysis.Tokenattributes;

namespace Raven.Client.Tests.Indexes
{
    public class LuceneAnalyzerUtils
    {
        public static IEnumerable<string> TokensFromAnalysis(Analyzer analyzer, String text)
        {
            TokenStream stream = analyzer.TokenStream("contents", new StringReader(text));
            List<string> result = new List<string>();
            TermAttribute tokenAttr = (TermAttribute)stream.GetAttribute(typeof(TermAttribute));

            while (stream.IncrementToken())
            {
                result.Add(tokenAttr.Term());
            }

            stream.End();
            stream.Close();

            return result;
        }
    }
}
