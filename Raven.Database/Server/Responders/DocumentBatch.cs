using System;
using System.Collections.Generic;
using System.Linq;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Database.Data;
using Raven.Database.Json;
using Raven.Database.Server.Abstractions;

namespace Raven.Database.Server.Responders
{
    public class DocumentBatch : RequestResponder
    {
        public override string UrlPattern
        {
            get { return @"/bulk_docs(/(.+))?"; }
        }

        public override string[] SupportedVerbs
        {
            get { return new[] { "POST", "PATCH", "DELETE" }; }
        }

        public override void Respond(IHttpContext context)
        {
            switch (context.Request.HttpMethod)
            {                
                case "POST":
                    Batch(context);
                    break;
				case "DELETE":
					OnBulkOperation(context, (docId, tx) =>
					{
						Database.Delete(docId, null, tx);
						return new { Document = docId, Deleted = true };
				
					} );
            		break;
				case "PATCH":
					var patchRequestJson = context.ReadJsonArray();
					var patchRequests = patchRequestJson.Cast<JObject>().Select(PatchRequest.FromJson).ToArray();
					OnBulkOperation(context, (docId, tx) =>
					{
						var patchResult = Database.ApplyPatch(docId, null, patchRequests, tx);
						return new {Document = docId, Result = patchResult};
					});
            		break;
            }
        }

    	private void OnBulkOperation(IHttpContext context, Func<string, TransactionInformation, object> batchOperation)
    	{
    		var match = urlMatcher.Match(context.GetRequestUrl());
    		var index = match.Groups[2].Value;
    		if (string.IsNullOrEmpty(index))
    		{
    			context.SetStatusToBadRequest();
    			return;
    		}
    		var allowStale = context.GetAllowStale();
    		Database.TransactionalStorage.Batch(actions =>
    		{
    			bool stale;
                var indexQuery = Index.GetIndexQueryFromHttpContext(context);

                indexQuery.PageSize = int.MaxValue; // get all
                indexQuery.FieldsToFetch = new[] { "__document_id" };

                var queryResults = Database.QueryDocumentIds(index, indexQuery, out stale);

    			if (stale)
    			{
    				context.SetStatusToNonAuthoritativeInformation();
    				if (allowStale == false)
    				{
    					throw new InvalidOperationException(
    						"Bulk operation cancelled because the index is stale and allowStale is false");
    				}
    			}

				var transactionInformation = GetRequestTransaction(context);
    			var array = new JArray();
				foreach (var documentId in queryResults)
    			{
    				var result = batchOperation(documentId, transactionInformation);
					array.Add(JObject.FromObject(result, new JsonSerializer { Converters = { new JsonEnumConverter() } }));
    			}
    			context.WriteJson(array);
    		});
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
