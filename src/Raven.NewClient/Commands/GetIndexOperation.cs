using Sparrow.Json;
using Sparrow.Logging;

namespace Raven.NewClient.Client.Commands
{
    public class GetIndexOperation
    {
        private readonly JsonOperationContext _context;
        private static readonly Logger _logger = LoggingSource.Instance.GetLogger<PutIndexOperation>("Raven.NewClient.Client");

        public BlittableJsonReaderArray IndexDefinitionResult;

        public GetIndexOperation(JsonOperationContext context)
        {
            _context = context;
        }

        protected void Log(string indexName)
        {
            if (_logger.IsInfoEnabled)
                _logger.Info($"Get '{indexName}' index");
        }

        public GetIndexCommand CreateRequest(string indexName = null)
        {
            return new GetIndexCommand()
            {
                Context = _context,
                IndexName = indexName
            };
        }

        public void SetResult(BlittableArrayResult result)
        {
            if (result == null)
            {
                IndexDefinitionResult = null;
                return;
            }
            IndexDefinitionResult = result.Results;
        }
    }
}