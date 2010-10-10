using System;
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
		    context.IndexStorage.RemoveFromIndex(Index, Keys, context);
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