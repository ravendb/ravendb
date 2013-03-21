using System;
using System.Collections.Generic;
using Raven.Database.Impl;
using Raven.Json.Linq;

namespace Raven.Database.Storage
{
	public interface IListsStorageActions
	{
		void Set(string name, string key, RavenJObject data, UuidType uuidType);
		
		void Remove(string name, string key);

		IEnumerable<ListItem> Read(string name, Guid start, Guid? end, int take);

		ListItem Read(string name, string key);

		void RemoveAllBefore(string name, Guid etag);
	}

	public class ListItem
	{
		public string Key;
		public Guid Etag;
		public RavenJObject Data;
	}
}