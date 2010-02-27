using Rhino.DivanDB.Indexing;
using System.Linq;
using Rhino.DivanDB.Json;

namespace Rhino.DivanDB.Tasks
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
            context.TransactionaStorage.Batch(actions =>
            {
                context.IndexStorage.Index(View, viewFunc, actions.DocumentsById(FromKey, ToKey, 100)
                                                               .Select(d =>
                                                               {
                                                                   lastId = d.Id;
                                                                   return d.Document;
                                                               })
                                                               .Where(x => x != null)
                                                               .Select(s => new JsonDynamicObject(s)));
                actions.Commit();
            });

            if (lastId < ToKey && lastId != -1)
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