using System;
using Raven.Database.Server.Abstractions;
using Raven.Database.Extensions;

namespace Raven.Database.Server.Responders.Admin
{
	public class AdminChangeDbId : AdminResponder
	{
		public override void RespondToAdmin(IHttpContext context)
		{
			Guid old = Database.TransactionalStorage.Id;
			var id = Database.TransactionalStorage.ChangeId();

			context.WriteJson(new
			{
				OldId = old,
				NewId = id
			});
		}
	}
}