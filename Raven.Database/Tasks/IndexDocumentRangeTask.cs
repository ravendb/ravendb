using System.Linq;
using Raven.Database.Extensions;
using Raven.Database.Indexing;
using Raven.Database.Json;

namespace Raven.Database.Tasks
{
    /// <summary>
    /// Indexing a range of documents
    /// A range of documents is a stable range, which can be queried even if document addition / removal
    /// occured since the time that the range was taken to the time it was queried.
    /// </summary>
    public class IndexDocumentRangeTask : Task
    {
        public int FromId { get; set; }
        public int ToId { get; set; }

        public override string ToString()
        {
            return string.Format("IndexDocumentRangeTask - View: {0}, FromId: {1}, ToId: {2}", 
                View, FromId, ToId);
        }

        public override void Execute(WorkContext context)
        {
            var viewFunc = context.IndexDefinitionStorage.GetIndexingFunction(View);
            if (viewFunc == null)
                return; // view was deleted, probably
            int lastId = FromId;
            var hasMoreItems = new Reference<bool>();
            context.TransactionaStorage.Batch(actions =>
            {
                var docsToIndex = actions.DocumentsById(hasMoreItems, FromId, ToId, 100)
                    .Select(d =>
                    {
                        lastId = d.Item2;
                        return d.Item1;
                    })
                    .Where(x => x != null)
                    .Select(s => JsonToExpando.Convert(s.ToJson()));
                context.IndexStorage.Index(View, viewFunc, docsToIndex);
                actions.Commit();
            });

            if (hasMoreItems.Value)
            {
                context.TransactionaStorage.Batch(actions =>
                {
                    actions.AddTask(new IndexDocumentRangeTask
                    {
                        FromId = lastId,
                        ToId = ToId,
                        View = View
                    });
                    actions.Commit();
                });
            }
        }
    }
}