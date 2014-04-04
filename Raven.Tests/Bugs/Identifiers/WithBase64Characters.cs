using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Raven.Client;
using Raven.Client.Document;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Bugs.Identifiers
{
	public class WithBase64Characters : RavenTest
	{
		public class Entity
		{
			public string Id { get; set; }
		}

		[Fact]
		public void Can_load_entity()
		{
			var specialId = "SHA1-UdVhzPmv0o+wUez+Jirt0OFBcUY=";

			using (base.GetNewServer())
			{
				IDocumentStore documentStore = new DocumentStore
				{
					Url = "http://localhost:8079"
				}.Initialize();

				using(var store = documentStore)
				{
					store.Initialize();

					using (var session = store.OpenSession())
					{
						var entity = new Entity() { Id = specialId };
						session.Store(entity);
						session.SaveChanges();
					}

					using (var session = store.OpenSession())
					{
						var entity1 = session.Load<object>(specialId);
						Assert.NotNull(entity1);
					}
				}
			}
		}
	}
}
