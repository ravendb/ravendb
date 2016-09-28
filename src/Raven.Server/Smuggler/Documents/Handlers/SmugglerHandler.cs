// -----------------------------------------------------------------------
//  <copyright file="SmugglerHandler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.IO;
using System.IO.Compression;
using System.Net.Http;
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
                    await Database.Operations.AddOperation("Export database: " + Database.Name, DatabaseOperations.PendingOperationType.DatabaseExport,
                        onProgress => Task.Run(() => ExportDatabaseInternal(context, exporter, onProgress, token), token.Token), operationId.Value, token);
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
            DocumentsOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            {
                var tuple = await GetImportStream();
                using(tuple.Item2)
                using (var stream = new GZipStream(tuple.Item1,CompressionMode.Decompress))
                {
                    await DoImport(context, stream);
                }
            }
        }

        private async Task<Tuple<Stream, IDisposable>> GetImportStream()
        {
            var file = GetStringQueryString("file", required: false);
            if (string.IsNullOrEmpty(file) == false)
                return Tuple.Create<Stream,IDisposable>(File.OpenRead(file),null);

            var url = GetStringQueryString("url", required: false);
            if (string.IsNullOrEmpty(url))
            {
                var httpClient = new HttpClient();
                
                var stream = await httpClient.GetStreamAsync(url);
                return Tuple.Create<Stream, IDisposable>(stream, httpClient);
            }

            return Tuple.Create<Stream, IDisposable>(HttpContext.Request.Body, null);
        }

        private async Task DoImport(DocumentsOperationContext context, Stream stream)
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