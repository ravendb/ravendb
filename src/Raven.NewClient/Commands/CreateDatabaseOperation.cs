using System;
using System.Text;
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
        private string _databaseName;
        public CreateDatabaseOperation(JsonOperationContext context)
        {
            _context = context;
        }

        protected void LogCreateDatabase()
        {
            if (_logger.IsInfoEnabled)
                _logger.Info($"Database '{_databaseName}' created");
        }

        public CreateDatabaseCommand CreateRequest(DocumentStore documentStore, DatabaseDocument databaseDocument)
        {
            _databaseName = databaseDocument.Id;
            if (databaseDocument.Settings.ContainsKey("Raven/DataDir") == false)
                throw new InvalidOperationException("The Raven/DataDir setting is mandatory");
            MultiDatabase.AssertValidName(databaseDocument.Id);

            var entityToBlittable = new EntityToBlittable(null);
            var databaseDocumentAsBlittable = entityToBlittable.ConvertEntityToBlittable(databaseDocument,
                documentStore.Conventions, _context);
            return new CreateDatabaseCommand()
            {
                DatabaseDocument = databaseDocumentAsBlittable,
                Context = _context
            };
        }
    }
}