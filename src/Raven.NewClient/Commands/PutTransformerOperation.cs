using System;
using Raven.NewClient.Abstractions.Data;
using Raven.NewClient.Abstractions.Indexing;
using Raven.NewClient.Client.Blittable;
using Raven.NewClient.Client.Document;
using Raven.NewClient.Client.Extensions;
using Raven.NewClient.Client.Indexing;
using Sparrow.Json;
using Sparrow.Logging;

namespace Raven.NewClient.Client.Commands
{
    public class PutTransformerOperation
    {
        private readonly JsonOperationContext _context;
        private static readonly Logger _logger = LoggingSource.Instance.GetLogger<PutTransformerOperation>("Raven.NewClient.Client");

        public PutTransformerOperation(JsonOperationContext context)
        {
            _context = context;
        }

        protected void Log()
        {
            if (_logger.IsInfoEnabled)
            {
            }
        }

        public PutTransformerCommand CreateRequest(DocumentConvention documentConvention, string transformerName, TransformerDefinition transformerDefinition)
        {
            var entityToBlittable = new EntityToBlittable(null);
            var transformerDefinitionAsBlittable = entityToBlittable.ConvertEntityToBlittable(transformerDefinition,
                documentConvention, _context);

            return new PutTransformerCommand()
            {
                TransformerDefinition = transformerDefinitionAsBlittable,
                Context = _context,
                TransformerName = transformerName
            };
        }

        public void SetResult(PutTransformerResult result)
        {
            
        }
    }
}