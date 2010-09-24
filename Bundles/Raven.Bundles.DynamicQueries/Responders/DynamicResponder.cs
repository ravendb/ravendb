using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Raven.Database.Server.Responders;
using Raven.Bundles.DynamicQueries.Data;
using Raven.Database.Data;
using Raven.Database;
using Raven.Bundles.DynamicQueries.Database;

namespace Raven.Bundles.DynamicQueries.Responders
{
    public class DynamicResponder : RequestResponder
    {
        public DocumentDatabase Database { get; set; }

        public override string UrlPattern
        {
            get { return "/dynamicquery$"; }
        }

        public override string[] SupportedVerbs
        {
            get { return new[] { "GET" }; }
        }
        
        public override void Respond(Raven.Database.Server.Abstractions.IHttpContext context)
        {
            var query = new DynamicQuery
			{
				Query = Uri.UnescapeDataString(context.Request.QueryString["query"] ?? ""),
                Start = context.GetStart(),
				PageSize = context.GetPageSize(Database.Configuration.MaxPageSize),
				FieldsToFetch = context.Request.QueryString.GetValues("fetch")
			};

            var queryResult = Database.ExecuteDynamicQuery(query);
            context.WriteJson(queryResult);
        }
    }
}
