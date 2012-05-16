using System.Collections.Generic;
using System.IO;
using System.Text;
using Lucene.Net.Analysis;
using Lucene.Net.Analysis.Tokenattributes;
using Lucene.Net.Index;
using Lucene.Net.Util;

namespace Raven.Bundles.MoreLikeThis
{
	class RavenMoreLikeThis : Similarity.Net.MoreLikeThis
	{
		private readonly IndexReader _ir;

		public RavenMoreLikeThis(IndexReader ir)
			: base(ir)
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
							AddTermFrequencies(new StringReader(t), termFreqMap, fieldName);
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
	}
}
