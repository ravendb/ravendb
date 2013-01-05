// -----------------------------------------------------------------------
//  <copyright file="OperationStatus.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Database.Server.Abstractions;
using Raven.Database.Extensions;

namespace Raven.Database.Server.Responders
{
	public class OperationStatus : AbstractRequestResponder
	{
		public override string UrlPattern
		{
			get { return "^/operation/status$"; }
		}
		public override string[] SupportedVerbs
		{
			get { return new[] { "GET" }; }
		}

		public override void Respond(IHttpContext context)
		{
			var idStr = context.Request.QueryString["id"];
			long id;
			if (long.TryParse(idStr, out id) == false)
			{
				context.SetStatusToBadRequest();
				context.WriteJson(new
				{
					Error  = "Query string variable id must be a valid int64"
				});
			}

			var status = Database.GetTaskState(id);
			if (status == null)
			{
				context.SetStatusToNotFound();
				return;
			}

			context.WriteJson(status);
		}
	}
}