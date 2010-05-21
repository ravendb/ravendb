using System.Linq;
using Raven.Database.Indexing;
using Raven.Database.Json;

namespace Raven.Database.Tasks
{
	public class ReduceTask : Task
	{
		public string ReduceKey { get; set; }

	    public override bool TryMerge(Task task)
		{
			var reduceTask = ((ReduceTask)task);
			// if the reduce key is the same, either task will have the same effect
			return reduceTask.ReduceKey == ReduceKey; 
		}

		public override void Execute(WorkContext context)
		{
			var viewGenerator = context.IndexDefinitionStorage.GetViewGenerator(Index);
			if (viewGenerator == null)
				return; // deleted view?

			context.TransactionaStorage.Batch(actions =>
			{
                var mappedResults = actions.GetMappedResults(Index, ReduceKey, MapReduceIndex.ComputeHash(Index, ReduceKey))
					.Select(JsonToExpando.Convert);

				context.IndexStorage.Reduce(Index, viewGenerator, mappedResults, context, actions, ReduceKey);
			});
		}

		public override Task Clone()
		{
			return new ReduceTask
			{
				Index = Index,
				ReduceKey = ReduceKey
			};
		}
	}
}