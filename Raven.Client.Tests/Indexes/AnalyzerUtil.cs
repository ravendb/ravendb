using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Lucene.Net.Analysis;

namespace Raven.Client.Tests.Indexes
{
    public class LuceneAnalyzerUtils
    {
        public static Token[] TokensFromAnalysis(Analyzer analyzer, String text)
        {
            TokenStream stream = analyzer.TokenStream("contents", new StringReader(text));
            ArrayList tokenList = new ArrayList();

            while (true)
            {
                Token token = stream.Next();
                if (token == null)
                {
                    break;
                }
                tokenList.Add(token);
            }
            return (Token[])tokenList.ToArray(typeof(Token));
        }
    }
}
