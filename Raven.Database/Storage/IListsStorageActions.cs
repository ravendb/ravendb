using System;
using System.Collections.Generic;
using Raven.Json.Linq;

namespace Raven.Database.Storage
{
	public interface IListsStorageActions
	{
		void Add(string name, string key, RavenJObject data);
		
		void Remove(string name, string key);

		IEnumerable<Tuple<Guid, RavenJObject>> Read(string name, Guid start, int take);

		RavenJObject Read(string name, string key);
	}
}