using System.Collections.Generic;
using System.Linq;
using Lucene.Net.Index;
using Lucene.Net.Search;
using Raven.Abstractions.Data;

namespace Raven.Database.Indexing
{
	public class IntersectionCollector : Collector
	{
		readonly Dictionary<string, SubQueryResult> results = new Dictionary<string, SubQueryResult>();
		private int currentBase;
		private IndexReader currentReader;
		private Scorer currentScorer;

		public IntersectionCollector(Searchable indexSearcher, IEnumerable<ScoreDoc> scoreDocs)
		{
			foreach (var scoreDoc in scoreDocs)
			{
				var document = indexSearcher.Doc(scoreDoc.doc);
				var subQueryResult = new SubQueryResult
				{
					LuceneId = scoreDoc.doc,
					RavenDocId = document.Get(Constants.DocumentIdFieldName) ?? document.Get(Constants.ReduceKeyFieldName),
					Score = float.IsNaN(scoreDoc.score) ? 0.0f : scoreDoc.score,
					Count = 1
				};
				results[subQueryResult.RavenDocId] = subQueryResult;
			}
		}
		
		public override void SetScorer(Scorer scorer)
		{
			currentScorer = scorer;
		}
		
		public override void Collect(int doc)
		{
			//Don't need to add the currentBase here, it's already accounted for
			var document = currentReader.Document(doc);
			var key = document.Get(Constants.DocumentIdFieldName) ?? document.Get(Constants.ReduceKeyFieldName);
			var currentScore = currentScorer.Score();

			SubQueryResult value;
			if (results.TryGetValue(key, out value))
			{
				value.Count++;
				value.Score += currentScore;
			}
		}

		public override void SetNextReader(IndexReader reader, int docBase)
		{
			currentReader = reader;
			currentBase = docBase;
		}
		
		public override bool AcceptsDocsOutOfOrder()
		{
			return true;
		}

		public IEnumerable<SubQueryResult> DocumentsIdsForCount(int expectedCount)
		{
			return from value in results.Values
				where value.Count >= expectedCount
				select value;
		}

		public class SubQueryResult
		{
			public int LuceneId { get; set; }
			public string RavenDocId { get; set; }
			public float Score { get; set; }
			public int Count { get; set; }
		}
	}
}