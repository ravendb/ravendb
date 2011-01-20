//-----------------------------------------------------------------------
// <copyright file="CascadeDeleteTrigger.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using Raven.Database;
using Raven.Database.Plugins;
using Raven.Http;

namespace Raven.Tests.Triggers
{
	public class CascadeDeleteTrigger : AbstractDeleteTrigger 
	{
        public override VetoResult AllowDelete(string key, TransactionInformation transactionInformation)
		{
			return VetoResult.Allowed;
		}

		public override void OnDelete(string key, TransactionInformation transactionInformation)
		{
			var document = Database.Get(key, null);
			if (document == null)
				return;
            Database.Delete(document.Metadata.Value<string>("Cascade-Delete"), null, null);
		}

		public override void AfterCommit(string key)
		{
		}
	}
}
