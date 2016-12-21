using Raven.NewClient.Client.Blittable;
using Raven.NewClient.Client.Document;
using Raven.NewClient.Client.Indexing;
using Sparrow.Json;
using Sparrow.Logging;

namespace Raven.NewClient.Client.Commands
{
    public class PutIndexOperation
    {
        private readonly JsonOperationContext _context;
        private static readonly Logger _logger = LoggingSource.Instance.GetLogger<PutIndexOperation>("Raven.NewClient.Client");

        public PutIndexOperation(JsonOperationContext context)
        {
            _context = context;
        }

        protected void Log(string indexName)
        {
            if (_logger.IsInfoEnabled)
                _logger.Info($"Put '{indexName}' index");
        }

        public PutIndexCommand CreateRequest(DocumentConvention documentConvention, string indexName, IndexDefinition indexDefinition)
        {
            var entityToBlittable = new EntityToBlittable(null);
            var indexDefinitionAsBlittable = entityToBlittable.ConvertEntityToBlittable(indexDefinition,
                documentConvention, _context);

            return new PutIndexCommand()
            {
                IndexDefinition = indexDefinitionAsBlittable,
                Context = _context,
                IndexName = indexName
            };
        }

        public void SetResult(PutIndexResult result)
        {
            
        }
    }
}