using System;

namespace Corax
{
	public class IndexingConventions
	{
		public delegate float IdfCalc(long docFreq, long numDocs);
		public delegate float ScorerCalc(float queryWeight, int termFreq, float boost);

		public delegate float TfCalc(int termFreq);

		public IdfCalc Idf = (docFreq, numDocs) => (float) (Math.Log(numDocs/(double) (docFreq + 1)) + 1.0);

		public TfCalc Tf = freq => (float)Math.Sqrt(freq);
	    public bool AutoCompact = true;
	}
}