using System;
using System.IO;
using System.Linq;
using System.Reflection;
using log4net.Appender;
using log4net.Config;
using log4net.Layout;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Raven.Database.Extensions;
using Raven.Database.Indexing;
using Xunit;

namespace Raven.Client.Tests.Document
{
	public class Game : RemoteClientTest, IDisposable
	{
		private string path;

		#region IDisposable Members

		public void Dispose()
		{
            IOExtensions.DeleteDirectory(path);
		}

		#endregion


		private DocumentStore NewDocumentStore()
		{
			path = Path.GetDirectoryName(Assembly.GetAssembly(typeof(DocumentStoreServerTests)).CodeBase);
			path = Path.Combine(path, "TestDb").Substring(6);
			var documentStore = new DocumentStore
			{
				Configuration =
				{
					DataDirectory = path
				}
			};
			documentStore.Initialize();
			return documentStore;
		}


		/// <summary>
		/// http://groups.google.com/group/ravendb/browse_thread/thread/e9f045e073d7a698
		/// </summary>
		[Fact]
		public void WillNotGetDuplicatedResults()
		{
			using (var store = NewDocumentStore())
			{
				BasicConfigurator.Configure(new OutputDebugStringAppender
				{
					Layout = new SimpleLayout()
				});
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
            Count = g.Sum(x => (int)x.Count).ToString()      
        };"
									});

				using (var documentSession = store.OpenSession())
				{
					documentSession.Store(new GameEvent()
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

					documentSession.Store(new GameEvent()
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

					documentSession.Store(new GameEvent()
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

					documentSession.Store(new GameEvent()
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

					documentSession.Store(new GameEvent()
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

					documentSession.Store(new GameEvent()
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

					documentSession.Store(new GameEvent()
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

					documentSession.Store(new GameEvent()
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

					documentSession.Store(new GameEvent()
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

					documentSession.Store(new GameEvent()
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

					var darykalSumResults =
                        documentSession.Advanced.LuceneQuery<GameEvent>("GameEventCountZoneBySpecificCharacter")
							.Where("CharacterName:Darykal AND RealmName:Moonglade AND Region:SingleRegion AND DataUploadId:10 ")
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
			using (var store = NewDocumentStore())
			{
				BasicConfigurator.Configure(new OutputDebugStringAppender
				{
					Layout = new SimpleLayout()
				});
				store.DatabaseCommands.PutIndex("GameEventCountZoneBySpecificCharacter",
					new IndexDefinition<GameEvent, GameEventCount>
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

				using (var documentSession = store.OpenSession())
				{
					documentSession.Store(new GameEvent()
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

					documentSession.Store(new GameEvent()
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

					documentSession.Store(new GameEvent()
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

					documentSession.Store(new GameEvent()
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

					documentSession.Store(new GameEvent()
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

					documentSession.Store(new GameEvent()
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

					documentSession.Store(new GameEvent()
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

					documentSession.Store(new GameEvent()
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

					documentSession.Store(new GameEvent()
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

					documentSession.Store(new GameEvent()
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

					var darykalSumResults =
                        documentSession.Advanced.LuceneQuery<GameEvent>("GameEventCountZoneBySpecificCharacter")
							.Where("CharacterName:Darykal AND RealmName:Moonglade AND Region:SingleRegion AND DataUploadId:10 ")
							.SelectFields<ZoneCountResult>("Zone", "Count")
							.WaitForNonStaleResults(TimeSpan.FromDays(1))
							.ToArray();

					Assert.Equal(3, darykalSumResults.Length);

				}
			}
		}


		public class GameEvent
		{
			public string Id
			{
				get;
				set;
			}

			public string UserId
			{
				get;
				set;
			}

			public string Region
			{
				get;
				set;
			}

			public string CharacterName
			{
				get;
				set;
			}

			public string RealmName
			{
				get;
				set;
			}

			public string DataUploadId
			{
				get;
				set;
			}

			public string Time
			{
				get;
				set;
			}

			public string ActionName
			{
				get;
				set;
			}

			public string Zone
			{
				get;
				set;
			}

			public string SubZone
			{
				get;
				set;
			}
		}

		public class GameEventCount
		{
			public string Region
			{
				get;
				set;
			}

			public string CharacterName
			{
				get;
				set;
			}

			public string RealmName
			{
				get;
				set;
			}

			public string DataUploadId
			{
				get;
				set;
			}

			public string Zone
			{
				get;
				set;
			}

			public string SubZone
			{
				get;
				set;
			}

			public int Count { get; set; }
		}
	}

	public class ZoneCountResult
	{
		public string Zone { get; set; }
		public int Count { get; set; }
	}
}