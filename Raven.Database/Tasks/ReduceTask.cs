using System;
using System.Linq;
using Newtonsoft.Json.Linq;
using Raven.Database.Indexing;
using Raven.Database.Json;
using Raven.Database.Linq;

namespace Raven.Database.Tasks
{
    public class ReduceTask : Task
    {
        public override void Execute(WorkContext context)
        {
            var viewGenerator = context.IndexDefinitionStorage.GetViewGenerator(Index);
            if (viewGenerator == null)
                return;// deleted view?

            context.TransactionaStorage.Batch(actions =>
            {
                var mappedResults = actions.GetMappedResults(Index, ReduceKey)
                    .Select(JObject.Parse)
                    .Select(JsonToExpando.Convert);

                context.IndexStorage.Reduce(Index, viewGenerator, mappedResults,context, actions, ReduceKey);

                actions.Commit();
            });
        }

        public string ReduceKey { get; set; }
    }
}