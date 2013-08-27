// -----------------------------------------------------------------------
//  <copyright file="HiloTests.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;
using Raven.Client.Document;
using Raven.Json.Linq;
using Xunit;
using System.Linq;

namespace Raven.Tests.MailingList
{
	public class HiloTests : RavenTest
	{
		[Fact]
		public void CanUseServerPrefix()
		{
			using (var store = NewDocumentStore())
			{
				store.DatabaseCommands.Put(
					"Raven/Hilo/Users", null,
					new RavenJObject
					{
						{"Max", 32}
					},
					new RavenJObject());

				store.DatabaseCommands.Put(
					"Raven/ServerPrefixForHilo", null,
					new RavenJObject
					{
						{"ServerPrefix", "2,"}
					},
					new RavenJObject());

				var hiLoKeyGenerator = new HiLoKeyGenerator("Users", 32);

				var generateDocumentKey = hiLoKeyGenerator.GenerateDocumentKey(store.DatabaseCommands, new DocumentConvention(), new User());
				Assert.Equal("Users/2,33", generateDocumentKey);
			}
		}
		[Fact]
		public void HiloCannotGoDown()
		{
			using (var store = NewDocumentStore())
			{
				store.DatabaseCommands.Put(
					"Raven/Hilo/Users", null,
					new RavenJObject
					{
						{"Max", 32}
					},
					new RavenJObject());

				var hiLoKeyGenerator = new HiLoKeyGenerator("Users", 32);

				var ids = new HashSet<long> { hiLoKeyGenerator.NextId(store.DatabaseCommands) };

				store.DatabaseCommands.Put(
					"Raven/Hilo/Users", null,
					new RavenJObject
					{
						{"Max", 12}
					},
					new RavenJObject());


				for (int i = 0; i < 128; i++)
				{
					Assert.True(ids.Add(hiLoKeyGenerator.NextId(store.DatabaseCommands)), "Failed at " + i);
				}

				var list = ids.GroupBy(x => x).Select(g => new
				{
					g.Key,
					Count = g.Count()
				}).Where(x => x.Count > 1).ToList();

				Assert.Empty(list);
			}
		}

		[Fact]
		public void ShouldResolveConflictWithHighestNumber()
		{
			using (var server = GetNewServer())
			using (var store = new DocumentStore
								  {
									  Url = "http://localhost:8079"
								  }.Initialize())
			{
				server.Database.TransactionalStorage.Batch(accessor =>
				{
					accessor.Documents.AddDocument(
						"Raven/Hilo/Users/Conflict/1", null,
						new RavenJObject
						{
							{"Max", 32}
						},
						new RavenJObject());

					accessor.Documents.AddDocument(
						"Raven/Hilo/Users/Conflict/2", null,
						new RavenJObject
						{
							{"Max", 64}
						},
						new RavenJObject());

					accessor.Documents.AddDocument("Raven/Hilo/Users", null,
							new RavenJObject
							{
							{
								"Conflicts",
								new RavenJArray()
								{
									"Raven/Hilo/Users/Conflict/1",
									"Raven/Hilo/Users/Conflict/2"
								}
								}
							},
							new RavenJObject
							{
							{
								"@Http-Status-Code"
								,
								409
								},
							{
								"@Http-Status-Description"
								, "Conflicted doc"
								}
							});
				});

				var hiLoKeyGenerator = new HiLoKeyGenerator("Users", 32);
				var nextId = hiLoKeyGenerator.NextId(store.DatabaseCommands);
				Assert.Equal(65, nextId);
			}
		}


	}
}