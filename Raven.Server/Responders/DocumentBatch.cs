using System.Collections.Generic;
using System.Net;
using Newtonsoft.Json.Linq;
using System;
using Raven.Database.Data;
using System.Linq;
using Raven.Database.Json;

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
        		var key = jsonCommand["Key"].Value<string>();
        		switch (jsonCommand.Value<string>("Method"))
        		{
        			case "PUT":
        				commands.Add(new PutCommandData
        				{
        					Key = key,
        					Etag = GetEtagFromCommand(jsonCommand),
        					Document = jsonCommand["Document"] as JObject,
        					Metadata = jsonCommand["Metadata"] as JObject,
                            TransactionInformation = GetRequestTransaction(context)
        				});
        				break;
        			case "DELETE":
        				commands.Add(new DeleteCommandData
        				{
        					Key = key,
        					Etag = GetEtagFromCommand(jsonCommand),
                            TransactionInformation = GetRequestTransaction(context)
        				});
        				break;
					case "PATCH":
						commands.Add(new PatchCommandData
						{
							Key = key,
							Etag = GetEtagFromCommand(jsonCommand),
							TransactionInformation = GetRequestTransaction(context),
							Patches = jsonCommand
								.Value<JArray>("Patches")
								.Cast<JObject>()
								.Select(PatchRequest.FromJson)
								.ToArray()
						});
        				break;
        			default:
        				throw new ArgumentException("Batching only supports PUT, PATCH and DELETE.");
        		}
            }

            var batchResult = Database.Batch(commands);
            context.WriteJson(batchResult);
        }

    	private static Guid? GetEtagFromCommand(JToken jsonCommand)
    	{
			return jsonCommand["Etag"] != null && jsonCommand["Etag"].Value<string>() != null ? new Guid(jsonCommand["Etag"].Value<string>()) : (Guid?)null;
    	}
    }
}
