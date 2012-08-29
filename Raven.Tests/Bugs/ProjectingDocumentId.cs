//-----------------------------------------------------------------------
// <copyright file="ProjectingDocumentId.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using Raven.Abstractions.Indexing;
using Xunit;
using System.Linq;
using Raven.Client.Linq;

namespace Raven.Tests.Bugs
{
	public class ProjectingDocumentId : RavenTest
	{
		[Fact]
		public void WillUseConventionsToSetProjection()
		{
			using (var store = NewDocumentStore())
			{
				store.DatabaseCommands.PutIndex("test",
												new IndexDefinition
													{
														Map = "from doc in docs select new { doc.Name }",
														Stores = { { "Name", FieldStorage.Yes } }
													});

				using (var s = store.OpenSession())
				{
					s.Store(new User
								{
									Email = "ayende@example.org",
									Name = "ayende"
								});

					s.SaveChanges();
				}

				using (var s = store.OpenSession())
				{
					var nameAndId = s.Advanced.LuceneQuery<User>("test")
						.WaitForNonStaleResults()
						.SelectFields<NameAndId>("Name", "__document_id")
						.Single();

					Assert.Equal(nameAndId.Name, "ayende");
					Assert.Equal(nameAndId.Id, "users/1");
				}
			}
		}


		public class MyEntity
		{
			public Guid Id { get; set; }
			public string Name { get; set; }
			public string Description { get; set; }
		}

		[Fact]
		public void ShouldIdentifyDocumentIdAlsoWithProjectionRetrieveDocumentInternal()
		{
			using (var store = NewDocumentStore())
			{
				Guid id = Guid.NewGuid();
				using (var s = store.OpenSession())
				{
					s.Store(new MyEntity { Id = id, Name = "test", Description = "my test" });

					s.SaveChanges();
				}

				using (var s = store.OpenSession())
				{
					var results = s.Query<MyEntity>().Customize(x=>x.WaitForNonStaleResults());
					var l1 = results.ToList();
					Assert.Equal(id, l1[0].Id);

					var l2 = results.Select(x => new { x.Id, x.Name, x.Description }).ToList();
					Assert.Equal(id, l2[0].Id);

					// try partial pull
					var l3 = results.Select(x => new { x.Id, x.Name, }).ToList();
					Assert.Equal(id, l3[0].Id);
				}
			}
		}

		public class Artist
		{
			public string Id { get; set; }
			public string Name { get; set; }
		}

		public class Album
		{
			public string ArtistId { get; set; }
			public string Title { get; set; }
		}

		[Fact]
		public void projecting_id_only()
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					var artist = new Artist { Name = "foo" };
					session.Store(artist);

					var album = new Album
									{
										ArtistId = artist.Id,
										Title = "All the shows"
									};
					session.Store(album);

					session.SaveChanges();
				}

				WaitForIndexing(store);

				using (var session = store.OpenSession())
				{
					var artistIDs = (from artist in session.Query<Artist>()
									 where artist.Name == "foo"
									 select artist.Id).ToArray();

					var albums = (from album in session.Query<Album>()
								  where album.ArtistId.In(artistIDs)
								  select album).ToArray();

					Assert.Equal(1, albums.Length);
				}
			}
		}

		[Fact]
		public void simple_usage()
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					var artist = new Artist { Name = "foo" };
					session.Store(artist);

					var album = new Album
					{
						ArtistId = artist.Id,
						Title = "All the shows"
					};
					session.Store(album);

					session.SaveChanges();
				}

				WaitForIndexing(store);

				using (var session = store.OpenSession())
				{
					var artistIDs = (from artist in session.Query<Artist>()
									 where artist.Name == "foo"
									 select artist.Id).ToArray();

					Assert.NotEmpty(artistIDs);
				}
			}
		}

	}
}
