// -----------------------------------------------------------------------
//  <copyright file="OAuthPutTrigger.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Database.Plugins;

namespace Raven.Database.Server.Security.Triggers
{
	public class OAuthPutTrigger : AbstractPutTrigger
	{
		public override VetoResult AllowPut(string key, Raven.Json.Linq.RavenJObject document, Raven.Json.Linq.RavenJObject metadata, Raven.Abstractions.Data.TransactionInformation transactionInformation)
		{
			if (key != null && key.StartsWith("Raven/ApiKeys/") && Authentication.IsEnabled == false)
				return VetoResult.Deny("Cannot setup OAuth Authentication without a valid commercial license.");

			return VetoResult.Allowed;
		}
	}
}