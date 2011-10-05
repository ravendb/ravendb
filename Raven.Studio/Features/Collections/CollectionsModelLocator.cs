// -----------------------------------------------------------------------
//  <copyright file="CollectionsModelLocator.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Studio.Infrastructure;
using Raven.Studio.Models;
using Raven.Client.Connection.Async;

namespace Raven.Studio.Features.Collections
{
	public class CollectionsModelLocator : ModelLocatorBase<DatabaseCollectionsModel>
	{
		protected override void Load(DatabaseModel database, IAsyncDatabaseCommands asyncDatabaseCommands, Observable<DatabaseCollectionsModel> observable)
		{
			observable.Value = new DatabaseCollectionsModel(asyncDatabaseCommands);
		}
	}
}