using System;
using System.Collections.Generic;
using System.Linq;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Database.Data;
using Raven.Database.Extensions;
using Raven.Database.Impl;
using Raven.Database.Json;
using Raven.Http.Abstractions;
using Raven.Http.Extensions;

namespace Raven.Database.Server.Responders
{
    public class DocumentBatch : RequestResponder
    {
        public override string UrlPattern
        {
            get { return @"^/bulk_docs(/(.+))?"; }
        }

        public override string[] SupportedVerbs
        {
            get { return new[] { "POST", "PATCH", "DELETE" }; }
        }

        public override void Respond(IHttpContext context)
        {
        	var databaseBulkOperations = new DatabaseBulkOperations(Database, GetRequestTransaction(context));
        	switch (context.Request.HttpMethod)
            {                
                case "POST":
                    Batch(context);
                    break;
				case "DELETE":
					OnBulkOperation(context, databaseBulkOperations.DeleteByIndex);
            		break;
				case "PATCH":
					var patchRequestJson = context.ReadJsonArray();
					var patchRequests = patchRequestJson.Cast<JObject>().Select(PatchRequest.FromJson).ToArray();
					OnBulkOperation(context, (index, query, allowStale) =>
						databaseBulkOperations.UpdateByIndex(index, query, patchRequests, allowStale));
            		break;
            }
        }

    	private void OnBulkOperation(IHttpContext context, Func<string, IndexQuery, bool, JArray> batchOperation)
    	{
    		var match = urlMatcher.Match(context.GetRequestUrl());
    		var index = match.Groups[2].Value;
    		if (string.IsNullOrEmpty(index))
    		{
    			context.SetStatusToBadRequest();
    			return;
    		}
    		var allowStale = context.GetAllowStale();
    		var indexQuery = context.GetIndexQueryFromHttpContext(maxPageSize: int.MaxValue);

    		var array = batchOperation(index, indexQuery, allowStale);

			context.WriteJson(array);

    	}
		
    	private void Batch(IHttpContext context)
        {
            var jsonCommandArray = context.ReadJsonArray();

    		var transactionInformation = GetRequestTransaction(context);
    		var commands = (from JObject jsonCommand in jsonCommandArray
    		                select CommandDataFactory.CreateCommand(jsonCommand, transactionInformation)).ToList();

    		var batchResult = Database.Batch(commands);
            context.WriteJson(batchResult);
        }
    }
}
