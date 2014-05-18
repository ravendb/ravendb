//-----------------------------------------------------------------------
// <copyright file="Game.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Linq;
using Raven.Abstractions.Indexing;
using Raven.Client;
using Raven.Client.Embedded;
using Raven.Client.Indexes;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Document
{
	public class Game : RavenTest
	{
		/// <summary>
		/// http://groups.google.com/group/ravendb/browse_thread/thread/e9f045e073d7a698
		/// </summary>
		[Fact]
		public void WillNotGetDuplicatedResults()
		{
			using (EmbeddableDocumentStore store = NewDocumentStore())
			{
				store.DatabaseCommands.PutIndex("GameEventCountZoneBySpecificCharacter",
				                                new IndexDefinition
				                                {
					                                Map =
						                                @"from doc in docs where doc.DataUploadId != null 
				&& doc.RealmName != null 
				&& doc.Region != null 
				&& doc.CharacterName != null 
				&& doc.Zone != null 
				&& doc.SubZone != null
	select new
	{
		DataUploadId = doc.DataUploadId,
		RealmName = doc.RealmName,
		Region = doc.Region,
		CharacterName = doc.CharacterName,
		Zone = doc.Zone,
		Count = 1
	};",
					                                Reduce =
						                                @"from result in results
		group result by new
		{
			DataUploadId = result.DataUploadId,
			RealmName = result.RealmName,
			Region = result.Region,
			CharacterName = result.CharacterName,
			Zone = result.Zone
		} into g
		select new
		{
			DataUploadId = g.Key.DataUploadId,
			RealmName = g.Key.RealmName,
			Region = g.Key.Region,
			CharacterName = g.Key.CharacterName,
			Zone = g.Key.Zone,
			Count = g.Sum(x => (int)x.Count)
		};"
				                                });

				using (IDocumentSession documentSession = store.OpenSession())
				{
					documentSession.Store(new GameEvent

					{
						Id = "1",
						UserId = "UserId1",
						Time = "232",
						ActionName = "Something",
						CharacterName = "Darykal",
						DataUploadId = "10",
						RealmName = "Moonglade",
						Region = "SingleRegion",
						SubZone = "SubzoneOne",
						Zone = "ZoneOne"
					});


					documentSession.Store(new GameEvent

					{
						Id = "2",
						UserId = "UserId1",
						Time = "232",
						ActionName = "Something",
						CharacterName = "Darykal",
						DataUploadId = "10",
						RealmName = "Moonglade",
						Region = "SingleRegion",
						SubZone = "SubzoneOne",
						Zone = "ZoneOne"
					});


					documentSession.Store(new GameEvent

					{
						Id = "3",
						UserId = "UserId1",
						Time = "232",
						ActionName = "Something",
						CharacterName = "Darykal",
						DataUploadId = "10",
						RealmName = "Moonglade",
						Region = "SingleRegion",
						SubZone = "SubzoneOne",
						Zone = "ZoneOne"
					});


					documentSession.Store(new GameEvent

					{
						Id = "4",
						UserId = "UserId1",
						Time = "232",
						ActionName = "Something",
						CharacterName = "Darykal",
						DataUploadId = "10",
						RealmName = "Moonglade",
						Region = "SingleRegion",
						SubZone = "SubzoneOne",
						Zone = "ZoneOne"
					});


					documentSession.Store(new GameEvent

					{
						Id = "5",
						UserId = "UserId1",
						Time = "232",
						ActionName = "Something",
						CharacterName = "Darykal",
						DataUploadId = "10",
						RealmName = "Moonglade",
						Region = "SingleRegion",
						SubZone = "SubzoneOne",
						Zone = "ZoneOne"
					});


					documentSession.Store(new GameEvent

					{
						Id = "6",
						UserId = "UserId1",
						Time = "232",
						ActionName = "Something",
						CharacterName = "Darykal",
						DataUploadId = "10",
						RealmName = "Moonglade",
						Region = "SingleRegion",
						SubZone = "SubzoneOne",
						Zone = "ZoneTwo"
					});


					documentSession.Store(new GameEvent

					{
						Id = "7",
						UserId = "UserId1",
						Time = "232",
						ActionName = "Something",
						CharacterName = "Darykal",
						DataUploadId = "10",
						RealmName = "Moonglade",
						Region = "SingleRegion",
						SubZone = "SubzoneOne",
						Zone = "ZoneTwo"
					});


					documentSession.Store(new GameEvent

					{
						Id = "8",
						UserId = "UserId1",
						Time = "232",
						ActionName = "Something",
						CharacterName = "Darykal",
						DataUploadId = "10",
						RealmName = "Moonglade",
						Region = "SingleRegion",
						SubZone = "SubzoneOne",
						Zone = "ZoneThree"
					});


					documentSession.Store(new GameEvent

					{
						Id = "9",
						UserId = "UserId1",
						Time = "232",
						ActionName = "Something",
						CharacterName = "Darykal",
						DataUploadId = "10",
						RealmName = "Moonglade",
						Region = "SingleRegion",
						SubZone = "SubzoneOne",
						Zone = "ZoneThree"
					});


					documentSession.Store(new GameEvent

					{
						Id = "10",
						UserId = "UserId1",
						Time = "232",
						ActionName = "Something",
						CharacterName = "Darykal",
						DataUploadId = "10",
						RealmName = "Moonglade",
						Region = "SingleRegion",
						SubZone = "SubzoneOne",
						Zone = "ZoneOne"
					});


					documentSession.SaveChanges();


					ZoneCountResult[] darykalSumResults =
                        documentSession.Advanced.DocumentQuery<GameEvent>("GameEventCountZoneBySpecificCharacter")
							.Where("RealmName:Moonglade AND Region:SingleRegion AND DataUploadId:10 ")
							.SelectFields<ZoneCountResult>("Zone", "Count")
							.WaitForNonStaleResults(TimeSpan.FromDays(1))
							.ToArray();


					Assert.Equal(3, darykalSumResults.Length);
				}
			}
		}


		[Fact]
		public void WillNotGetDuplicatedResults_UsingLinq()

		{
			using (EmbeddableDocumentStore store = NewDocumentStore())

			{
				store.DatabaseCommands.PutIndex("GameEventCountZoneBySpecificCharacter",
				                                new IndexDefinitionBuilder<GameEvent, GameEventCount>

				                                {
					                                Map = docs =>
					                                      from doc in docs
					                                      where doc.DataUploadId != null
					                                            && doc.RealmName != null
					                                            && doc.Region != null
					                                            && doc.CharacterName != null
					                                            && doc.Zone != null
					                                            && doc.SubZone != null
					                                      select new

					                                      {
						                                      doc.DataUploadId,
						                                      doc.RealmName,
						                                      doc.Region,
						                                      doc.CharacterName,
						                                      doc.Zone,
						                                      Count = 1
					                                      },
					                                Reduce = results => from result in results
					                                                    group result by new

					                                                    {
						                                                    result.DataUploadId,
						                                                    result.RealmName,
						                                                    result.Region,
						                                                    result.CharacterName,
						                                                    result.Zone
					                                                    }
					                                                    into g
					                                                    select new

					                                                    {
						                                                    g.Key.DataUploadId,
						                                                    g.Key.RealmName,
						                                                    g.Key.Region,
						                                                    g.Key.CharacterName,
						                                                    g.Key.Zone,
						                                                    Count = g.Sum(x => x.Count)
					                                                    }
				                                });


				using (IDocumentSession documentSession = store.OpenSession())

				{
					for (int i = 0; i < 5; i++)

					{
						documentSession.Store(new GameEvent

						{
							Id = (i + 1).ToString(),
							UserId = "UserId1",
							Time = "232",
							ActionName = "Something",
							CharacterName = "Darykal",
							DataUploadId = "10",
							RealmName = "Moonglade",
							Region = "SingleRegion",
							SubZone = "SubzoneOne",
							Zone = "ZoneOne"
						});
					}


					for (int i = 6; i < 8; i++)

					{
						documentSession.Store(new GameEvent

						{
							Id = (i + 1).ToString(),
							UserId = "UserId1",
							Time = "232",
							ActionName = "Something",
							CharacterName = "Darykal",
							DataUploadId = "10",
							RealmName = "Moonglade",
							Region = "SingleRegion",
							SubZone = "SubzoneOne",
							Zone = "ZoneTwo"
						});
					}


					for (int i = 9; i < 12; i++)

					{
						documentSession.Store(new GameEvent

						{
							Id = (i + 1).ToString(),
							UserId = "UserId1",
							Time = "232",
							ActionName = "Something",
							CharacterName = "Darykal",
							DataUploadId = "10",
							RealmName = "Moonglade",
							Region = "SingleRegion",
							SubZone = "SubzoneOne",
							Zone = "ZoneThree"
						});
					}


					documentSession.SaveChanges();


					dynamic[] darykalSumResults =
                        documentSession.Advanced.DocumentQuery<dynamic>("GameEventCountZoneBySpecificCharacter")
							.Where("CharacterName:Darykal AND RealmName:Moonglade AND Region:SingleRegion AND DataUploadId:10 ")
							.WaitForNonStaleResults(TimeSpan.FromDays(1))
							.ToArray();


					Assert.Equal(3, darykalSumResults.Length);
				}
			}
		}

		#region Nested type: GameEvent

		public class GameEvent

		{
			public string Id { get; set; }


			public string UserId { get; set; }


			public string Region { get; set; }


			public string CharacterName { get; set; }


			public string RealmName { get; set; }


			public string DataUploadId { get; set; }


			public string Time { get; set; }


			public string ActionName { get; set; }


			public string Zone { get; set; }


			public string SubZone { get; set; }
		}

		#endregion

		#region Nested type: GameEventCount

		public class GameEventCount

		{
			public string Region { get; set; }


			public string CharacterName { get; set; }


			public string RealmName { get; set; }


			public string DataUploadId { get; set; }


			public string Zone { get; set; }


			public string SubZone { get; set; }


			public int Count { get; set; }
		}

		#endregion
	}
}
