using Raven.Database.Server.Abstractions;
using Raven.Database.Extensions;

namespace Raven.Database.Server.Responders
{
	public class Identity : AbstractRequestResponder
	{
		public override string UrlPattern
		{
			get { return "^/identity/next$"; }
		}

		public override string[] SupportedVerbs
		{
			get { return new[] { "POST" }; }
		}

		public override void Respond(IHttpContext context)
		{
			var name = context.Request.QueryString["name"];
			if(string.IsNullOrWhiteSpace(name))
			{
				context.SetStatusToBadRequest();
				context.WriteJson(new
				{
					Error = "'name' query string parameter is mandatory and cannot be empty"
				});
				return;
			}

			long nextIdentityValue = -1;
			Database.TransactionalStorage.Batch(accessor =>
			{
				nextIdentityValue = accessor.General.GetNextIdentityValue(name);
			});
			context.WriteJson(new
			{
				Value = nextIdentityValue
			});
		}
	}
}