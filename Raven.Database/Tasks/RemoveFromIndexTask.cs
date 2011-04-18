//-----------------------------------------------------------------------
// <copyright file="RemoveFromIndexTask.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using Raven.Database.Indexing;
using System.Linq;

namespace Raven.Database.Tasks
{
	public class RemoveFromIndexTask : Task
	{
		public string[] Keys { get; set; }

		public override string ToString()
		{
			return string.Format("Index: {0}, Keys: {1}", Index, string.Join(", ", Keys));
		}

		public override bool TryMerge(Task task)
		{
			var removeFromIndexTask = ((RemoveFromIndexTask)task);
			Keys = Keys.Union(removeFromIndexTask.Keys).ToArray();
			return true;
		}

		public override void Execute(WorkContext context)
		{
			var keysToRemove = new HashSet<string>();
			context.TransactionaStorage.Batch(accessor =>
			{
				foreach (var key in
					from key in Keys
					let documentByKey = accessor.Documents.DocumentByKey(key, null)
					where documentByKey == null
					select key)
				{
					keysToRemove.Add(key);
				}
				var indexLastUpdatedAt = accessor.Staleness.IndexLastUpdatedAt(Index);
				accessor.Indexing.UpdateLastIndexed(Index, indexLastUpdatedAt.Item2, DateTime.Now);
			});
			context.IndexStorage.RemoveFromIndex(Index, keysToRemove.ToArray(), context);
		}

		public override Task Clone()
		{
			return new RemoveFromIndexTask
			{
				Keys = Keys.ToArray(),
				Index = Index,
			};
		}
	}
}
