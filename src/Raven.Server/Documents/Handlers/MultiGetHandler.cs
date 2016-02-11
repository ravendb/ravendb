// -----------------------------------------------------------------------
//  <copyright file="MultiGetHandler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Primitives;
using Raven.Abstractions.Data;
using Raven.Server.Json;
using Raven.Server.Json.Parsing;
using Raven.Server.Routing;
using Sparrow;

namespace Raven.Server.Documents
{
    public class MultiGetHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/multi_get", "POST", "/databases/{databaseName:string}/document?id={documentId:string}")]
        public async Task PostMultiGet()
        {
            RavenOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            {
                var requests = await context.ParseArrayToMemory(RequestBodyStream(), "multi_get", BlittableJsonDocumentBuilder.UsageMode.None);
                for (int i = 0; i < requests.Length; i++)
                {
                    var request = requests[i] as BlittableJsonReaderObject;
                    if (request == null)
                        continue;

                    string method = "GET", url, query;
                    if (request.TryGet("Url", out url) == false || 
                        request.TryGet("Query", out query) == false)
                        continue;

                    await Server.Router.HandlePath(HttpContext, method, url);
                }
            }
        }
    }
}