//-----------------------------------------------------------------------
// <copyright file="FilterRavenInternalDocumentsReadTrigger.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using Raven.Abstractions.Data;
using Raven.Json.Linq;

namespace Raven.Database.Plugins.Builtins
{
	public class FilterRavenInternalDocumentsReadTrigger : AbstractReadTrigger
	{
		public override ReadVetoResult AllowRead(string key, RavenJObject metadata, ReadOperation operation, TransactionInformation transactionInformation)
		{
			if(key == null)
				return ReadVetoResult.Allowed;
			if (key.StartsWith("Raven/",StringComparison.OrdinalIgnoreCase))
			{
				switch (operation)
				{
					case ReadOperation.Load:
						return ReadVetoResult.Allowed;
					case ReadOperation.Query:
					case ReadOperation.Index:
						return ReadVetoResult.Ignore;
					default:
						throw new ArgumentOutOfRangeException("operation");
				}
			}
			return ReadVetoResult.Allowed;
		}
	}
}
