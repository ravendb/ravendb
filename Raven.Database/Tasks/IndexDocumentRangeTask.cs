using System.Linq;
using Raven.Database.Extensions;
using Raven.Database.Indexing;
using Raven.Database.Json;

namespace Raven.Database.Tasks
{
    public class IndexDocumentRangeTask : Task
    {
        public int FromKey { get; set; }
        public int ToKey { get; set; }

        public override string ToString()
        {
            return string.Format("IndexDocumentRangeTask - View: {0}, FromKey: {1}, ToKey: {2}", View, FromKey, ToKey);
        }

        public override void Execute(WorkContext context)
        {
            var viewFunc = context.IndexDefinitionStorage.GetIndexingFunction(View);
            if (viewFunc == null)
                return; // view was deleted, probably
            int lastId = FromKey;
            var hasMoreItems = new Reference<bool>();
            context.TransactionaStorage.Batch(actions =>
            {
                var docsToIndex = actions.DocumentsById(hasMoreItems, FromKey, ToKey, 100)
                    .Select(d =>
                    {
                        lastId = d.Second;
                        return d.First;
                    })
                    .Where(x => x != null)
                    .Select(s => new JsonDynamicObject(s.ToJson()));
                context.IndexStorage.Index(View, viewFunc, docsToIndex);
                actions.Commit();
            });

            if (hasMoreItems.Value)
            {
                context.TransactionaStorage.Batch(actions =>
                {
                    actions.AddTask(new IndexDocumentRangeTask
                    {
                        FromKey = lastId,
                        ToKey = ToKey,
                        View = View
                    });
                    actions.Commit();
                });
            }
        }
    }
}