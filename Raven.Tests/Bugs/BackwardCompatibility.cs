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

		public class Partial
		{
			public string Id { get; set; }
			public string Name { get; set; }
		}
	}
}