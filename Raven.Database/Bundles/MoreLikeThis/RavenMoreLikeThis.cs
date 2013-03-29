using System.Collections.Generic;
using System.IO;
using Lucene.Net.Index;
using Lucene.Net.Util;

namespace Raven.Database.Bundles.MoreLikeThis
{
	class RavenMoreLikeThis : Lucene.Net.Search.Similar.MoreLikeThis
	{
		private readonly IndexReader _ir;

		public RavenMoreLikeThis(IndexReader ir)
			: base(ir)
		{
			_ir = ir;
		}

		protected override PriorityQueue<object[]> RetrieveTerms(int docNum)
		{
			var fieldNames = GetFieldNames();

			IDictionary<string, Int> termFreqMap = new Lucene.Net.Support.HashMap<string, Int>();
			
			foreach (var fieldName in fieldNames)
			{
				var vector = _ir.GetTermFreqVector(docNum, fieldName);

				// field does not store term vector info
				if (vector == null)
				{
					var d = _ir.Document(docNum);
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
