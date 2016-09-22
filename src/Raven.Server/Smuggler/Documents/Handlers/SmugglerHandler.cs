// -----------------------------------------------------------------------
//  <copyright file="SmugglerHandler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.IO.Compression;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Data;
using Raven.Client.Smuggler;
using Raven.Server.Documents;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Smuggler.Documents.Handlers
{
    public class SmugglerHandler : DatabaseRequestHandler
    {

        [RavenAction("/databases/*/smuggler/validateOptions", "POST")]
        public Task PostValidateOptions()
        {
            //TODO: implement me!

            
            return Task.CompletedTask;
        }

        [RavenAction("/databases/*/smuggler/export", "POST")]
        public async Task PostExport()
        {
            DocumentsOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            using (context.OpenReadTransaction())
            {
                var operationId = GetIntValueQueryString("operationId", required: false);

                var exporter = new SmugglerExporter(Database)
                {
                    DocumentsLimit = GetIntValueQueryString("documentsLimit", required: false),
                    RevisionDocumentsLimit = GetIntValueQueryString("RevisionDocumentsLimit", required: false),
                };

                var operateOnTypes = GetStringQueryString("operateOnTypes", required: false);
                DatabaseItemType databaseItemType;
                if (Enum.TryParse(operateOnTypes, true, out databaseItemType))
                {
                    exporter.OperateOnTypes = databaseItemType;
                }

                var token = CreateOperationToken();

                if (operationId.HasValue)
                {
                    await Database.DatabaseOperations.AddOperation("Export database: " + Database.Name, DatabaseOperations.PendingOperationType.DatabaseExport, 
                        onProgress => Task.Run(() => ExportDatabaseInternal(context, exporter, onProgress, token)), operationId.Value, token);
                }
                else
                {
                    ExportDatabaseInternal(context, exporter, null, token);
                }
            }
        }

        private IOperationResult ExportDatabaseInternal(DocumentsOperationContext context, SmugglerExporter exporter, Action<IOperationProgress> onProgress, OperationCancelToken token)
        {
            try
            {
               


                //TODO: use optional onProgress parameter
                exporter.Export(context, ResponseBodyStream());
                return null; //TODO: pass operation result to operation status
            }
            finally
            {
                token.Dispose();
            }
        }

        [RavenAction("/databases/*/smuggler/import", "POST")]
        public async Task PostImport()
        {
            // var fileName = GetQueryStringValueAndAssertIfSingleAndNotEmpty("fileName");
            DocumentsOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            //TODO: detect gzip or not based on query string param
            using (var stream = new GZipStream(HttpContext.Request.Body, CompressionMode.Decompress))
            {
                var importer = new SmugglerImporter(Database);

                var operateOnTypes = GetStringQueryString("operateOnTypes", required: false);
                DatabaseItemType databaseItemType;
                if (Enum.TryParse(operateOnTypes, true, out databaseItemType))
                {
                    importer.OperateOnTypes = databaseItemType;
                }

                await importer.Import(context, stream);
            }
        }
    }
}