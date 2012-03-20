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
		private List<SubQueryResult> skippedItems = new List<SubQueryResult>();

		public IntersectionCollector(Searchable indexSearcher, IEnumerable<ScoreDoc> scoreDocs)
		{
			foreach (var scoreDoc in scoreDocs)
			{
				var document = indexSearcher.Doc(scoreDoc.doc);
				var subQueryResult = new SubQueryResult
				{
					LuceneId = scoreDoc.doc,
					RavenDocId = document.Get(Constants.DocumentIdFieldName) ?? document.Get(Constants.ReduceKeyFieldName),
					Score = scoreDoc.score,
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
			var document = currentReader.Document(currentBase + doc);
			var key = document.Get(Constants.DocumentIdFieldName) ?? document.Get(Constants.ReduceKeyFieldName);
			var currentScore = currentScorer.Score();

			SubQueryResult value;
			if (results.TryGetValue(key, out value))
			{
				value.Count++;
				value.Score += currentScore;
			}
			else
			{
				skippedItems.Add(new SubQueryResult
									{
										LuceneId = currentBase + doc,
										RavenDocId = key,
										Score = currentScore,
										Count = 1
									});
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
				where value.Count == expectedCount
				select value;
		}

		public void UpdateInitialItems(Searchable indexSearcher, IEnumerable<ScoreDoc> docsToAdd)
		{
			foreach (var scoreDoc in docsToAdd)
			{
				var document = indexSearcher.Doc(scoreDoc.doc);
				var subQueryResult = new SubQueryResult
				{
					LuceneId = scoreDoc.doc,
					RavenDocId = document.Get(Constants.DocumentIdFieldName) ?? document.Get(Constants.ReduceKeyFieldName),
					Score = scoreDoc.score,
					Count = 1
				};
				if (results.ContainsKey(subQueryResult.RavenDocId) == false)
					results[subQueryResult.RavenDocId] = subQueryResult;
			}

			var copyOfSkippedItems = new List<SubQueryResult>(skippedItems);
			skippedItems.Clear();
			foreach (var skippedDoc in copyOfSkippedItems)
			{
				SubQueryResult value;
				if (results.TryGetValue(skippedDoc.RavenDocId, out value))
				{
					value.Count++;
					value.Score += skippedDoc.Score;
				}
				else
				{
					skippedItems.Add(skippedDoc);
				}
			}
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