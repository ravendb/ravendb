using Raven.NewClient.Client.Blittable;
using Raven.NewClient.Client.Data;
using Raven.NewClient.Client.Data.Queries;
using Raven.NewClient.Client.Document;
using Sparrow.Json;
using Sparrow.Logging;

namespace Raven.NewClient.Client.Commands
{
    public class PatchByIndexOperation
    {
        private readonly JsonOperationContext _context;
        private static readonly Logger _logger = LoggingSource.Instance.GetLogger<PatchByIndexOperation>("Raven.NewClient.Client");

        public PatchByIndexOperation(JsonOperationContext context)
        {
            _context = context;
        }

        protected void LogPatchByIndex(string indexName)
        {
            if (_logger.IsInfoEnabled)
                _logger.Info($"Patch by '{indexName}' index");
        }

        public PatchByIndexCommand CreateRequest(string indexName, IndexQuery queryToUpdate, QueryOperationOptions options, 
            PatchRequest patch, DocumentStore documentStore)
        {
            var entityToBlittable = new EntityToBlittable(null);
            var requestData = entityToBlittable.ConvertEntityToBlittable(patch, documentStore.Conventions, _context);

            return new PatchByIndexCommand()
            {
                Script = requestData,
                IndexName = indexName,
                QueryToUpdate = queryToUpdate,
                Options = options,
                Context = _context
            };
        }
    }
}