//-----------------------------------------------------------------------
// <copyright file="FailDelete.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using Raven.Client;
using Raven.Client.Listeners;
using Raven.Json.Linq;

namespace Raven.Tests.Bugs
{
	public class FailDelete : IDocumentDeleteListener
	{
		public void BeforeDelete(string key, object entityInstance, RavenJObject metadata)
		{
			throw new NotImplementedException();
		}
	}
}