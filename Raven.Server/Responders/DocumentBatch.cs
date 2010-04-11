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
            ICommandData[] commandData = new ICommandData[jsonCommandArray.Count];

            for (int commandIndex = 0; commandIndex < jsonCommandArray.Count; commandIndex++)
            {

                if (jsonCommandArray[commandIndex]["method"].Value<string>() == "PUT")
                {
                    commandData[commandIndex] = new PutCommandData
                    {
                        Key = jsonCommandArray[commandIndex]["key"].Value<string>(),
						Etag = GetEtagFromCommand(jsonCommandArray[commandIndex]),
                        Document = jsonCommandArray[commandIndex]["document"] as JObject,
                        Metadata = jsonCommandArray[commandIndex]["@meta"] as JObject,
                    };
                    continue;
                }

                if (jsonCommandArray[commandIndex]["method"].Value<string>() == "DELETE")
                {
                    commandData[commandIndex] = new DeleteCommandData
                    {
                        Key = jsonCommandArray[commandIndex]["key"].Value<string>(),
						Etag = GetEtagFromCommand(jsonCommandArray[commandIndex]),
                    };
                    continue;
                }

                throw new ArgumentException("Batching only supports PUT and DELETE.");
            }

            var batchResult = Database.Batch(commandData);
            context.WriteJson(batchResult);
        }

    	private static Guid? GetEtagFromCommand(JToken jsonCommand)
    	{
    		return jsonCommand["etag"] != null ? new Guid(jsonCommand["etag"].Value<string>()) : (Guid?)null;
    	}
    }
}
