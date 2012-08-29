//-----------------------------------------------------------------------
// <copyright file="GeneralStorageActions.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.Collections.Generic;
using Raven.Database.Storage;
using Raven.Json.Linq;
using Raven.Storage.Managed.Impl;

namespace Raven.Storage.Managed
{
	public class GeneralStorageActions : IGeneralStorageActions
	{
		private readonly TableStorage storage;

		public GeneralStorageActions(TableStorage storage)
		{
			this.storage = storage;
		}

		public long GetNextIdentityValue(string name)
		{
			var result = storage.Identity.Read(new RavenJObject {{"name", name}});
			if(result == null)
			{
				storage.Identity.UpdateKey(new RavenJObject {{"name", name}, {"id", 1}});
				return 1;
			}
			var val = result.Key.Value<int>("id") + 1;
			storage.Identity.UpdateKey(new RavenJObject { { "name", name }, { "id", val } });
			return val;
		}
	}
}