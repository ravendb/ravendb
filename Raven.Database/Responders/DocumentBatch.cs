using System.Collections.Generic;
using Newtonsoft.Json.Linq;
using Raven.Database.Abstractions;
using Raven.Database.Data;

namespace Raven.Database.Responders
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

        public override void Respond(IHttpContext context)
        {
            switch (context.Request.HttpMethod)
            {                
                case "POST":
                    Batch(context);
                    break;
            }
        }

        private void Batch(IHttpContext context)
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
