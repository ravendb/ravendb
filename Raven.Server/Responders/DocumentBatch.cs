using System.Collections.Generic;
using System.Net;
using Newtonsoft.Json.Linq;
using System;
using Raven.Database;
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

			var transactionInformation = GetRequestTransaction(context);
			foreach (JObject jsonCommand in jsonCommandArray)
			{
				commands.Add(CommandDataFactory.CreateCommand(jsonCommand, transactionInformation));
			}

        	var batchResult = Database.Batch(commands);
            context.WriteJson(batchResult);
        }
    }
}
