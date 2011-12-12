//-----------------------------------------------------------------------
// <copyright file="RemoveFromIndexTask.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using Raven.Abstractions.Data;
using Raven.Database.Indexing;
using System.Linq;

namespace Raven.Database.Tasks
{
	public class RemoveFromIndexTask : Task
	{
		public HashSet<string> Keys { get; set; }

		public override string ToString()
		{
			return string.Format("Index: {0}, Keys: {1}", Index, string.Join(", ", Keys));
		}

		public RemoveFromIndexTask()
		{
			Keys = new HashSet<string>(StringComparer.InvariantCultureIgnoreCase);
		}

		public override bool TryMerge(Task task)
		{
			var removeFromIndexTask = ((RemoveFromIndexTask)task);
			Keys.UnionWith(removeFromIndexTask.Keys);
			return true;
		}

		public override void Execute(WorkContext context)
		{
			var keysToRemove = new HashSet<string>(StringComparer.InvariantCultureIgnoreCase);
			context.TransactionaStorage.Batch(accessor =>
			{
				keysToRemove = new HashSet<string>(Keys.Where(key=>accessor.Documents.DocumentMetadataByKey(key, null) == null));
				accessor.Indexing.TouchIndexEtag(Index);
			});
			context.IndexStorage.RemoveFromIndex(Index, keysToRemove.ToArray(), context);
		}

		public override Task Clone()
		{
			return new RemoveFromIndexTask
			{
				Keys = new HashSet<string>(Keys),
				Index = Index,
			};
		}
	}
}
