//-----------------------------------------------------------------------
// <copyright file="DocumentBatch.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Linq;
using Newtonsoft.Json.Linq;
using Raven.Database.Data;
using Raven.Database.Extensions;
using Raven.Database.Impl;
using Raven.Database.Json;
using Raven.Http.Abstractions;
using Raven.Http.Extensions;
using Raven.Json.Linq;

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
					var patchRequests = patchRequestJson.Cast<RavenJObject>().Select(PatchRequest.FromJson).ToArray();
					OnBulkOperation(context, (index, query, allowStale) =>
						databaseBulkOperations.UpdateByIndex(index, query, patchRequests, allowStale));
            		break;
            }
        }

    	private void OnBulkOperation(IHttpContext context, Func<string, IndexQuery, bool, RavenJArray> batchOperation)
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
    		var commands = (from RavenJObject jsonCommand in jsonCommandArray
    		                select CommandDataFactory.CreateCommand(jsonCommand, transactionInformation))
    			.ToArray();

			context.Log(log =>
			{
				if (log.IsDebugEnabled)
				{
					foreach (var commandData in commands)
					{
						log.DebugFormat("\t{0} {1}", commandData.Method, commandData.Key);
					}
				}
			});

    		var batchResult = Database.Batch(commands);
            context.WriteJson(batchResult);
        }
    }
}
