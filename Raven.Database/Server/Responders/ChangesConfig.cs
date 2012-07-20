// -----------------------------------------------------------------------
//  <copyright file="ChangesConfig.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Database.Server.Abstractions;
using Raven.Database.Extensions;

namespace Raven.Database.Server.Responders
{
	public class ChangesConfig : AbstractRequestResponder
	{
		public override string UrlPattern
		{
			get { return "^/changes/config$"; }
		}
		
		public override string[] SupportedVerbs
		{
			get { return new[] {"POST"}; }
		}

		public override void Respond(IHttpContext context)
		{
			var id = context.Request.QueryString["id"];
			if(string.IsNullOrEmpty(id))
			{
				context.SetStatusToBadRequest();
				context.WriteJson(new
				                  	{
				                  		Error = "id query string parameter is mandatory when using changes/config endpoint"
				                  	});
				return;
			}

			var jsonData = context.ReadJson();
			var name = jsonData.Value<string>("Name");
			var connectionState = Database.TransportState.For(id);
			switch (jsonData.Value<string>("Type"))
			{
				case "WatchIndex":
					connectionState.WatchIndex(name);
					break;
				case "UnwatchIndex":
					connectionState.UnwatchIndex(name);
					break;

				case "WatchDocument":
					connectionState.WatchDocument(name);
					break;
				case "UnwatchDocument":
					connectionState.UnwatchDocument(name);
					break;

				case "WatchAllDocuments":
					connectionState.WatchAllDocuments();
					break;
				case "UnwatchAllDocuments":
					connectionState.UnwatchAllDocuments();
					break;

				case "WatchDocumentPrefix":
					connectionState.WatchDocumentPrefix(name);
					break;
				case "UnwatchDocumentPrefix":
					connectionState.UnwatchDocumentPrefix(name);
					break;
			}
		}
	}
}