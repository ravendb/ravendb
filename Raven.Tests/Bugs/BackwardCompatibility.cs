using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using Raven.Abstractions.Data;
using Raven.Client.Listeners;
using Raven.Json.Linq;
using Raven.Tests.Common;

using Xunit;
using System.Linq;

namespace Raven.Tests.Bugs
{
	public class BackwardCompatibility : RavenTest
	{
		
		[Fact]
		public void WillNotLoseInformation()
		{
			using(var store = NewDocumentStore())
			{
				store.DatabaseCommands.Put("partials/1", null,
				                           RavenJObject.FromObject(new {Name = "Ayende", Email = "ayende@ayende.com"}),
				                           new RavenJObject());

				store.RegisterListener(new BackwardCompatibilityListener());

				using(var session = store.OpenSession())
				{
					var entity = session.Load<Partial>("partials/1");
					entity.Name = "Ayende Rahien";
					session.SaveChanges();
				}

				var doc = store.DatabaseCommands.Get("partials/1");
				Assert.Equal("Ayende Rahien", doc.DataAsJson.Value<string>("Name"));
				Assert.Equal("ayende@ayende.com", doc.DataAsJson.Value<string>("Email"));
			}
		}

		public class BackwardCompatibilityListener : IDocumentConversionListener
		{
			readonly ConcurrentDictionary<Type, HashSet<string>> typePropertiesCache = new ConcurrentDictionary<Type, HashSet<string>>();
			readonly ConditionalWeakTable<object, Dictionary<string, RavenJToken>> missingProps = new ConditionalWeakTable<object, Dictionary<string, RavenJToken>>();

			public void EntityToDocument(string key, object entity, RavenJObject document, RavenJObject metadata)
			{
				Dictionary<string, RavenJToken> value;
				if (missingProps.TryGetValue(entity, out value) == false)
					return;
				foreach (var kvp in value)
				{
					document[kvp.Key] = kvp.Value;
				}
			}

			public void DocumentToEntity(string key, object entity, RavenJObject document, RavenJObject metadata)
			{
				var hashSet = typePropertiesCache.GetOrAdd(entity.GetType(), type => new HashSet<string>(type.GetProperties().Select(x => x.Name)));
				foreach (var propNotOnEntity in document.Keys.Where(s => hashSet.Contains(s) == false))
				{
					missingProps.GetOrCreateValue(entity)[propNotOnEntity] = document[propNotOnEntity];
				}
			}
		}

		public class Partial
		{
			public string Id { get; set; }
			public string Name { get; set; }
		}
	}
}