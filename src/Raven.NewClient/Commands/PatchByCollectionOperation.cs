using System;
using Raven.NewClient.Client.Blittable;
using Raven.NewClient.Client.Data;
using Raven.NewClient.Client.Data.Queries;
using Raven.NewClient.Client.Document;
using Sparrow.Json;
using Sparrow.Logging;

namespace Raven.NewClient.Client.Commands
{
    public class PatchByCollectionOperation
    {
        private readonly JsonOperationContext _context;
        private static readonly Logger _logger = LoggingSource.Instance.GetLogger<PatchByCollectionOperation>("Raven.NewClient.Client");

        public PatchByCollectionOperation(JsonOperationContext context)
        {
            _context = context;
        }

        protected void LogPatchByCollection(string collectionName)
        {
            //TODO - Better log info
            if (_logger.IsInfoEnabled)
                _logger.Info($"Patch by '{collectionName}' collecion");
        }

        public PatchByCollectionCommand CreateRequest(string collectionName, PatchRequest patch, DocumentStore documentStore)
        {
            var entityToBlittable = new EntityToBlittable(null);
            var requestData = entityToBlittable.ConvertEntityToBlittable(patch, documentStore.Conventions, _context);

            return new PatchByCollectionCommand()
            {
                Script = requestData,
                Context = _context,
                CollectionName = collectionName
            };
        }
    }
}