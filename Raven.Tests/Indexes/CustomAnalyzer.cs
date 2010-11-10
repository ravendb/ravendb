using System;
using System.IO;
using Lucene.Net.Analysis;

namespace Raven.Tests.Indexes
{
    public class CustomAnalyzer : KeywordAnalyzer
    {
        public override TokenStream TokenStream(string fieldName, TextReader reader)
        {
            return new LowerCaseFilter(new ASCIIFoldingFilter(base.TokenStream(fieldName, reader)));
        }
    }
}