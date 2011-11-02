using System.Collections.Generic;
using System.IO;
using System.Text;
using Lucene.Net.Analysis;
using Lucene.Net.Index;
using Lucene.Net.Util;

namespace Raven.Bundles.MoreLikeThis
{
    class RavenMoreLikeThis : Similarity.Net.MoreLikeThis
    {
        private readonly IndexReader _ir;

        public Dictionary<string, Analyzer> Analyzers { get; set; }

        public RavenMoreLikeThis(IndexReader ir) : base(ir)
        {
            _ir = ir;
        }

        protected override PriorityQueue RetrieveTerms(int docNum)
        {
            var fieldNames = GetFieldNames();
            
            var termFreqMap = new System.Collections.Hashtable();
            var d = _ir.Document(docNum);
            foreach (var fieldName in fieldNames)
            {
                var vector = _ir.GetTermFreqVector(docNum, fieldName);

                // field does not store term vector info
                if (vector == null)
                {
                    var text = d.GetValues(fieldName);
                    if (text != null)
                    {
                        foreach (var t in text)
                        {
                            var byteArray = Encoding.ASCII.GetBytes(t);
                            var stream = new MemoryStream(byteArray);
                            AddTermFrequencies(new StreamReader(stream), termFreqMap, fieldName);
                        }
                    }
                }
                else
                {
                    AddTermFrequencies(termFreqMap, vector);
                }
            }

            return CreateQueue(termFreqMap);
        }

        protected new void AddTermFrequencies(System.IO.StreamReader r, System.Collections.IDictionary termFreqMap, System.String fieldName)
        {
            var analyzer = Analyzers[fieldName];
            TokenStream ts = analyzer.TokenStream(fieldName, r);
            Token token;
            int tokenCount = 0;
            while ((token = ts.Next()) != null)
            {
                // for every token
                System.String word = token.TermText();
                tokenCount++;
                if (tokenCount > GetMaxNumTokensParsed())
                {
                    break;
                }
                if (IsNoiseWord(word))
                {
                    continue;
                }

                // increment frequency
                var cnt = (Int)termFreqMap[word];
                if (cnt == null)
                {
                    termFreqMap[word] = new Int();
                }
                else
                {
                    cnt.x++;
                }
            }
        }
    }
}
