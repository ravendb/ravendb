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
            var match = urlMatcher.Match(context.Request.Url.LocalPath);
            var docId = match.Groups[1].Value;
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
        					Metadata = jsonCommand["@meta"] as JObject,
        				});
        				break;
        			case "DELETE":
        				commands.Add(new DeleteCommandData
        				{
        					Key = key.Value<string>(),
        					Etag = GetEtagFromCommand(jsonCommand),
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
    		return jsonCommand["etag"] != null ? new Guid(jsonCommand["etag"].Value<string>()) : (Guid?)null;
    	}
    }
}
