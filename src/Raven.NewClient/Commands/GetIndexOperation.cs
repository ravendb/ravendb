using System;
using Raven.NewClient.Abstractions.Data;
using Raven.NewClient.Client.Blittable;
using Raven.NewClient.Client.Document;
using Raven.NewClient.Client.Extensions;
using Raven.NewClient.Client.Indexing;
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

        protected void Log()
        {
            if (_logger.IsInfoEnabled)
            {
            }
        }

        public GetIndexCommand CreateRequest(string indexName = null)
        {
            return new GetIndexCommand()
            {
                Context = _context,
                IndexName = indexName
            };
        }

        public void SetResult(GetIndexResult result)
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