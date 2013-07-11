using System;
using Raven.Database.Extensions;
using Raven.Database.Server.Abstractions;

namespace Raven.Database.Server.Responders
{
	public class IdentitySeed : AbstractRequestResponder
	{
		public override string UrlPattern
		{
			get { return "^/identity/seed"; }
		}

		public override string[] SupportedVerbs
		{
			get { return new[] { "POST" }; }
		}

		public override void Respond(IHttpContext context)
		{
			var name = context.Request.QueryString["name"];
			var valueString = context.Request.QueryString["value"];

			if (string.IsNullOrWhiteSpace(name)) {
				context.SetStatusToBadRequest();
				context.WriteJson(new
				{
					Error = "'name' query string parameter is mandatory and cannot be empty"
				});
				return;
			}

			if (string.IsNullOrWhiteSpace(valueString)) {
				context.SetStatusToBadRequest();
				context.WriteJson(new
				{
					Error = "'seed' query string parameter is mandatory and cannot be empty"
				});
			}

			long value;
			if (!Int64.TryParse(valueString, out value)) {
				context.SetStatusToBadRequest();
				context.WriteJson(new {
					Error = "'seed' query string parameter must be an integer"
				});
			}

			Database.TransactionalStorage.Batch(accessor => accessor.General.SetIdentityValue(name, value)); 
			context.WriteJson(new
			{
				Value = value
			});
		}
	}
}