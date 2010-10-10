using System.Collections;
using System.Collections.Generic;
using System.Linq;
using Raven.Database.Indexing;
using Raven.Database.Json;

namespace Raven.Database.Tasks
{
	public class ReduceTask : Task
	{
		public string[] ReduceKeys { get; set; }

		public override bool SupportsMerging
		{
			get
			{
				return ReduceKeys.Length < 128;
			}
		}

	    public override bool TryMerge(Task task)
		{
			var reduceTask = ((ReduceTask)task);
	    	ReduceKeys = ReduceKeys.Concat(reduceTask.ReduceKeys).Distinct().ToArray();
	    	return true;
		}

		public override void Execute(WorkContext context)
		{
			var viewGenerator = context.IndexDefinitionStorage.GetViewGenerator(Index);
			if (viewGenerator == null)
				return; // deleted view?

			context.TransactionaStorage.Batch(actions =>
			{
				IEnumerable<object> mappedResults = null;
				foreach (var reduceKey in ReduceKeys)
				{
					IEnumerable<object> enumerable = actions.MappedResults.GetMappedResults(Index, reduceKey, MapReduceIndex.ComputeHash(Index, reduceKey))
						.Select(JsonToExpando.Convert);

					if (mappedResults == null)
						mappedResults = enumerable;
					else
						mappedResults = mappedResults.Concat(enumerable);
				}

				context.IndexStorage.Reduce(Index, viewGenerator, mappedResults, context, actions, ReduceKeys);
			});
		}

		public override Task Clone()
		{
			return new ReduceTask
			{
				Index = Index,
				ReduceKeys = ReduceKeys
			};
		}
	}
}