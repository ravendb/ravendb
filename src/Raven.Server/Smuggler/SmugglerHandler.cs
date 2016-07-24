// -----------------------------------------------------------------------
//  <copyright file="SmugglerHandler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.IO.Compression;
using System.Threading.Tasks;
using Raven.Client.Smuggler;
using Raven.Server.Documents;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Smuggler
{
    public class SmugglerHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/smuggler/export", "POST")]
        public Task PostExport()
        {
            DocumentsOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            using (context.OpenReadTransaction())
            {
                var exporter = new DatabaseDataExporter(Database)
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
                
                exporter.Export(context, ResponseBodyStream());
            }
            return Task.CompletedTask;
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
                var importer = new DatabaseDataImporter(Database);

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