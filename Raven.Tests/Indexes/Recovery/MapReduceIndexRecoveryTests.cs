// -----------------------------------------------------------------------
//  <copyright file="MapReduceIndexRecoveryTests.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Globalization;
using System.IO;
using System.Linq;
using Raven.Client.Document;
using Raven.Database.Extensions;
using Xunit;

namespace Raven.Tests.Indexes.Recovery
{
	public class MapReduceIndexRecoveryTests : RavenTest
	{
		protected override void ModifyConfiguration(Database.Config.InMemoryRavenConfiguration configuration)
		{
			configuration.NumberOfItemsToExecuteReduceInSingleStep = 10;
		}

		[Fact]
		public void ShouldRegenerateMapReduceIndex()
		{
			var dataDir = NewDataPath("ShouldRegenerateMapReduceIndex");
			var index = new MapReduceRecoveryTestIndex();

			string indexFullPath;

			using (var server = GetNewServer(runInMemory: false, dataDirectory: dataDir))
			{
				using (var store = new DocumentStore { Url = "http://localhost:8079" }.Initialize())
				{
					index.Execute(store);

                    indexFullPath = Path.Combine(server.SystemDatabase.Configuration.IndexStoragePath,
                                             server.SystemDatabase.IndexStorage.GetIndexInstance(index.IndexName).IndexId.ToString(CultureInfo.InvariantCulture));

					using (var session = store.OpenSession())
					{
						// reduce in single step
						for (var i = 0; i < 5; i++)
						{
							session.Store(new Recovery
							{
								Name = "One",
								Number = 1
							});

							session.Store(new Recovery
							{
								Name = "Two",
								Number = 2
							});
						}


						// reduce in multiple step
						for (int i = 0; i < 100; i++)
						{
							session.Store(new Recovery
							{
								Name = "Three",
								Number = 3
							});

							session.Store(new Recovery
							{
								Name = "Four",
								Number = 4
							});
						}

						session.SaveChanges();
					}

					WaitForIndexing(store);
				}
			}

			IndexMessing.MessSegmentsFile(indexFullPath);

			using (GetNewServer(runInMemory: false, dataDirectory: dataDir)) // do not delete previous directory
			{
				using (var store = new DocumentStore { Url = "http://localhost:8079" }.Initialize())
				{
					using (var session = store.OpenSession())
					{
						var result =
							session.Query<Recovery, MapReduceRecoveryTestIndex>().Customize(x => x.WaitForNonStaleResults()).OrderBy(x => x.Number).ToList();

						Assert.Equal(4, result.Count);

						Assert.Equal("One", result[0].Name);
						Assert.Equal(5, result[0].Number);

						Assert.Equal("Two", result[1].Name);
						Assert.Equal(10, result[1].Number);

						Assert.Equal("Three", result[2].Name);
						Assert.Equal(300, result[2].Number);

						Assert.Equal("Four", result[3].Name);
						Assert.Equal(400, result[3].Number);
					}
				}
			}
		}
	}
}