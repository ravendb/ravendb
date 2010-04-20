using System.Collections.Generic;
using System.Net;
using Newtonsoft.Json.Linq;
using System;
using Raven.Database.Data;

namespace Raven.Server.Responders
{
    public class DocumentBatch : RequestResponder
    {
        public override string UrlPattern
        {
            get { return @"/bulk_docs"; }
        }

        public override string[] SupportedVerbs
        {
            get { return new[] { "POST" }; }
        }

        public override void Respond(HttpListenerContext context)
        {
            switch (context.Request.HttpMethod)
            {                
                case "POST":
                    Batch(context);
                    break;
            }
        }

        private void Batch(HttpListenerContext context)
        {
            var jsonCommandArray = context.ReadJsonArray();
        	var commands = new List<ICommandData>();

			foreach (JObject jsonCommand in jsonCommandArray)
        	{
        		var key = jsonCommand["key"];
        		switch (jsonCommand["method"].Value<string>())
        		{
        			case "PUT":
        				commands.Add(new PutCommandData
        				{
        					Key = key.Value<string>(),
        					Etag = GetEtagFromCommand(jsonCommand),
        					Document = jsonCommand["document"] as JObject,
        					Metadata = jsonCommand["@metadata"] as JObject,
                            TransactionInformation = GetRequestTransaction(context)
        				});
        				break;
        			case "DELETE":
        				commands.Add(new DeleteCommandData
        				{
        					Key = key.Value<string>(),
        					Etag = GetEtagFromCommand(jsonCommand),
                            TransactionInformation = GetRequestTransaction(context)
        				});
        				continue;
        			default:
        				throw new ArgumentException("Batching only supports PUT and DELETE.");
        		}
            }

            var batchResult = Database.Batch(commands);
            context.WriteJson(batchResult);
        }

    	private static Guid? GetEtagFromCommand(JToken jsonCommand)
    	{
			return jsonCommand["etag"] != null && jsonCommand["etag"].Value<string>() != null ? new Guid(jsonCommand["etag"].Value<string>()) : (Guid?)null;
    	}
    }
}
