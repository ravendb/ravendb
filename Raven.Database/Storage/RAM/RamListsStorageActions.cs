using System;
using System.Collections.Generic;
using Raven.Database.Impl;
using Raven.Json.Linq;
using System.Linq;

namespace Raven.Database.Storage.RAM
{
	public class RamListsStorageActions : IListsStorageActions
	{
		private readonly RamState state;
		private readonly IUuidGenerator generator;

		public RamListsStorageActions(RamState state, IUuidGenerator generator)
		{
			this.state = state;
			this.generator = generator;
		}

		public void Set(string name, string key, RavenJObject data)
		{
			state.Lists.GetOrAdd(name).Set(key, new ListItem
			{
				Data = data,
				Etag = generator.CreateSequentialUuid(),
				Key = key
			});
		}

		public void Remove(string name, string key)
		{
			state.Lists.GetOrAdd(name).Remove(key);
		}

		public IEnumerable<ListItem> Read(string name, Guid start, int take)
		{
			return state.Lists.GetOrAdd(name)
				.OrderBy(x => x.Value.Etag)
				.SkipWhile(x => x.Value.Etag.CompareTo(start) < 0)
				.Take(take)
				.Select(pair => pair.Value);
		}

		public ListItem Read(string name, string key)
		{
			return state.Lists.GetOrAdd(name).GetOrDefault(key);
		}
	}
}