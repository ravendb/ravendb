using Raven.Database.Indexing;
using Raven.Database.Json;

namespace Raven.Database.Tasks
{
    public class IndexDocumentTask : Task
    {
        public string Key { get; set; }

        public override string ToString()
        {
            return string.Format("IndexDocumentTask - Key: {0}", Key);
        }

        public override void Execute(WorkContext context)
        {
            context.TransactionaStorage.Batch(actions =>
            {
                var doc = actions.DocumentByKey(Key);
                if (doc == null)
                {
                    actions.Commit();
                    return;
                }

                var json = new JsonDynamicObject(doc.ToJson());

                foreach (var viewName in context.IndexDefinitionStorage.IndexNames)
                {
                    var viewFunc = context.IndexDefinitionStorage.GetIndexingFunction(viewName);
                    if (viewFunc != null)
                        context.IndexStorage.Index(viewName, viewFunc, new[] {json,});
                }

                actions.Commit();
            });
        }
    }
}