//-----------------------------------------------------------------------
// <copyright file="FailStore.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using Raven.Client;
using Raven.Client.Listeners;
using Raven.Json.Linq;

namespace Raven.Tests.Bugs
{
	public class FailStore : IDocumentStoreListener
	{
		public bool BeforeStore(string key, object entityInstance, RavenJObject metadata, RavenJObject original)
		{
			throw new NotImplementedException();
		}

		public void AfterStore(string key, object entityInstance, RavenJObject metadata)
		{
			throw new NotImplementedException();
		}
	}
}