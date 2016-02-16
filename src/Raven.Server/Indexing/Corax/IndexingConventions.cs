using System;

namespace Raven.Server.Indexing.Corax
{
    public class IndexingConventions
    {
        public delegate float IdfCalc(long docFreq, long numDocs);

        public delegate float TfCalc(int termFreq);

        public static IdfCalc Idf = (docFreq, numDocs) => (float) (Math.Log(numDocs/(double) (docFreq + 1)) + 1.0);

        public static TfCalc Tf = freq => (float)Math.Sqrt(freq);
        public bool AutoCompact = true;
    }
}