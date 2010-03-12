using System.Linq;
using Raven.Database.Extensions;
using Raven.Database.Indexing;
using Raven.Database.Json;

namespace Raven.Database.Tasks
{
    /// <summary>
    ///   Indexing a range of documents
    ///   A range of documents is a stable range, which can be queried even if document addition / removal
    ///   occured since the time that the range was taken to the time it was queried.
    /// </summary>
    public class IndexDocumentRangeTask : Task
    {
        public int FromId { get; set; }
        public int ToId { get; set; }

        public override string ToString()
        {
            return string.Format("IndexDocumentRangeTask - Index: {0}, FromId: {1}, ToId: {2}",
                                 Index, FromId, ToId);
        }

        public override void Execute(WorkContext context)
        {
            var viewGenerator = context.IndexDefinitionStorage.GetViewGenerator(Index);
            if (viewGenerator == null)
                return; // index was deleted, probably
            context.TransactionaStorage.Batch(actions =>
            {
                var docsToIndex = actions.DocumentsById(new Reference<bool>(), FromId, ToId, 100)
                    .Select(d => d.Item1)
                    .Where(x => x != null)
                    .Select(s => JsonToExpando.Convert(s.ToJson()));
                context.IndexStorage.Index(Index, viewGenerator, docsToIndex, context, actions);
                actions.Commit();
            });
        }
    }
}