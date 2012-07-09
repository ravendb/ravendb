using System.IO;
using Lucene.Net.Index;
using Lucene.Net.Util;

namespace Raven.Database.Bundles.MoreLikeThis
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
