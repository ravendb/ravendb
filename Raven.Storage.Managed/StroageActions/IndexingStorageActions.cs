using System;
using System.Collections.Generic;
using Newtonsoft.Json;
using Newtonsoft.Json.Bson;
using Raven.Database.Data;
using Raven.Database.Storage.StorageActions;

namespace Raven.Storage.Managed.StroageActions
{
	public class IndexingStorageActions : AbstractStorageActions,IIndexingStorageActions
	{
		private IndexStats current;

		private IndexStats Current
		{
			get
			{
				if(current == null)
					throw new InvalidOperationException("The current index was not set via SetCurrentIndexStatsTo");
				return current;
			}
		}

		public void SetCurrentIndexStatsTo(string index)
		{
			var indexPos = Mutator.Indexes.FindValue(index);
			if(indexPos==null)
				throw new InvalidOperationException("Could not find index named: " + index);
			Reader.Position = indexPos.Value;
			current = new JsonSerializer().Deserialize<IndexStats>(new BsonReader(Reader));
		}

		public void FlushIndexStats()
		{
			var position = Writer.Position;
			new JsonSerializer().Serialize(new BsonWriter(Writer),Current);
			Mutator.Indexes.Add(Current.Name, position);
		}

		public void IncrementIndexingAttempt()
		{
			Current.IndexingAttempts++;
		}

		public void IncrementSuccessIndexing()
		{
			Current.IndexingSuccesses++;
		}

		public void IncrementIndexingFailure()
		{
			Current.IndexingErrors++;
		}

		public void DecrementIndexingAttempt()
		{
			Current.IndexingAttempts--;
		}

		public IEnumerable<IndexStats> GetIndexesStats()
		{
			var jsonSerializer = new JsonSerializer();
			foreach (var treeNode in Viewer.Indexes.IndexScan())
			{
				if(treeNode.NodeValue == null)
					continue;
				Reader.Position = treeNode.NodeValue.Value;
				yield return jsonSerializer.Deserialize<IndexStats>(new BsonReader(Reader));
			}
		}

		public IndexFailureInformation GetFailureRate(string index)
		{
			var indexPos = Viewer.Indexes.FindValue(index);
			if (indexPos == null)
				throw new InvalidOperationException("Could not find index named: " + index);
			Reader.Position = indexPos.Value;
			var indexStats = new JsonSerializer().Deserialize<IndexStats>(new BsonReader(Reader));
			return new IndexFailureInformation
			{
				Attempts = indexStats.IndexingAttempts,
				Errors = indexStats.IndexingErrors,
				Successes = indexStats.IndexingSuccesses,
				Name = indexStats.Name
			};
		}

		public void AddIndex(string name)
		{
			if (Mutator.Indexes.FindValue(name) != null)
				throw new InvalidOperationException("Index already exists");
			var position = Writer.Position;
			new JsonSerializer().Serialize(new BsonWriter(Writer),
			                               new IndexStats
			                               {
			                               		Name = name
			                               });
			Mutator.Indexes.Add(name, position);
		}

		public void DeleteIndex(string name)
		{
			Mutator.Indexes.Remove(name);
		}
	}
}