using System;
using Raven.NewClient.Abstractions.Data;
using Raven.NewClient.Client.Blittable;
using Raven.NewClient.Client.Document;
using Raven.NewClient.Client.Extensions;
using Sparrow.Json;
using Sparrow.Logging;

namespace Raven.NewClient.Client.Commands
{
    public class CreateDatabaseOperation
    {
        private readonly JsonOperationContext _context;
        private static readonly Logger _logger = LoggingSource.Instance.GetLogger<CreateDatabaseOperation>("Raven.NewClient.Client");

        public InMemoryDocumentSessionOperations.SaveChangesData Data;

        public CreateDatabaseOperation(JsonOperationContext context)
        {
            _context = context;
        }

        protected void LogBatch()
        {
            if (_logger.IsInfoEnabled)
            {
               //TODO - Efrat
            }
        }

        public CreateDatabaseCommand CreateRequest(DocumentStore documentStore, DatabaseDocument databaseDocument)
        {
            //TODO -EFRAT - WIP
            if (databaseDocument.Settings.ContainsKey("Raven/DataDir") == false)
                throw new InvalidOperationException("The Raven/DataDir setting is mandatory");
            MultiDatabase.AssertValidName(databaseDocument.Id);

            JsonOperationContext jsonOperationContext;
            documentStore.GetRequestExecuter(databaseDocument.Id)
                .ContextPool.AllocateOperationContext(out jsonOperationContext);

            var entityToBlittable = new EntityToBlittable(null);
            var databaseDocumentAsBlittable = entityToBlittable.ConvertEntityToBlittable(databaseDocument,
                documentStore.Conventions, jsonOperationContext);
            return new CreateDatabaseCommand()
            {
                DatabaseDocument = databaseDocumentAsBlittable,
                Context = _context
            };
        }

        public void SetResult(CreateDatabaseResult result)
        {
            
        }
    }
}