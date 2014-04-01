// -----------------------------------------------------------------------
//  <copyright file="GroupByAndDocumentId.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client;
using Raven.Client.Indexes;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Linq
{
	using Xunit.Extensions;

	public class GroupByAndDocumentId : RavenTest
	{
		public class Client
		{
			public int Id { get; set; }
			public string Name { get; set; }
			public IList<ImportStatusMessage> ImportStatuses { get; set; }
		}

		public class ImportStatusMessage
		{
			public DateTime TimeStamp { get; set; }
			public ImportStatus Status { get; set; }
		}

		public enum ImportStatus
		{
			Complete,
			Running,
			Failed,
			Waiting,
			NoReport
		}

		[Theory]
		[PropertyData("Storages")]
		public void Test1(string requestedStorage)
		{
			DoTest<Client_ImportSummaryByDate_1>(requestedStorage);
		}

		[Theory]
		[PropertyData("Storages")]
		public void Test2(string requestedStorage)
		{
			DoTest<Client_ImportSummaryByDate_2>(requestedStorage);
		}

		[Theory]
		[PropertyData("Storages")]
		public void Test3(string requestedStorage)
		{
			DoTest<Client_ImportSummaryByDate_3>(requestedStorage);
		}

		[Theory]
		[PropertyData("Storages")]
		public void Test4(string requestedStorage)
		{
			DoTest<Client_ImportSummaryByDate_4>(requestedStorage);
		}

		private void DoTest<TIndex>(string requestedStorage)
			where TIndex : AbstractIndexCreationTask, new()
		{
			using (var documentStore = NewDocumentStore(requestedStorage: requestedStorage))
			{
				documentStore.ExecuteIndex(new TIndex());

				using (var session = documentStore.OpenSession())
				{
					session.Store(new Client
					{
						Name = "A",
						ImportStatuses = new List<ImportStatusMessage>
						{
							new ImportStatusMessage
							{
								Status = ImportStatus.Waiting,
								TimeStamp = new DateTime(2013, 1, 1, 0, 0, 0)
							},
							new ImportStatusMessage
							{
								Status = ImportStatus.Running,
								TimeStamp = new DateTime(2013, 1, 1, 1, 0, 0)
							},
							new ImportStatusMessage
							{
								Status = ImportStatus.Complete,
								TimeStamp = new DateTime(2013, 1, 1, 2, 0, 0)
							},
							new ImportStatusMessage
							{
								Status = ImportStatus.Waiting,
								TimeStamp = new DateTime(2013, 2, 2, 0, 0, 0)
							},
							new ImportStatusMessage
							{
								Status = ImportStatus.Running,
								TimeStamp = new DateTime(2013, 2, 2, 1, 0, 0)
							},
							new ImportStatusMessage
							{
								Status = ImportStatus.Complete,
								TimeStamp = new DateTime(2013, 2, 2, 2, 0, 0)
							},
						}
					});

					session.Store(new Client
					{
						Name = "B",
						ImportStatuses = new List<ImportStatusMessage>
						{
							new ImportStatusMessage
							{
								Status = ImportStatus.Waiting,
								TimeStamp = new DateTime(2013, 1, 1, 0, 0, 0)
							},
							new ImportStatusMessage
							{
								Status = ImportStatus.Running,
								TimeStamp = new DateTime(2013, 1, 1, 1, 0, 0)
							},
							new ImportStatusMessage
							{
								Status = ImportStatus.Complete,
								TimeStamp = new DateTime(2013, 1, 1, 2, 0, 0)
							},
							new ImportStatusMessage
							{
								Status = ImportStatus.Waiting,
								TimeStamp = new DateTime(2013, 2, 2, 0, 0, 0)
							},
							new ImportStatusMessage
							{
								Status = ImportStatus.Running,
								TimeStamp = new DateTime(2013, 2, 2, 1, 0, 0)
							},
							new ImportStatusMessage
							{
								Status = ImportStatus.Complete,
								TimeStamp = new DateTime(2013, 2, 2, 2, 0, 0)
							},
						}
					});

					session.SaveChanges();
				}

				WaitForIndexing(documentStore);

				AssertNoIndexErrors(documentStore);

				using (var session = documentStore.OpenSession())
				{
					var results = session.Query<ImportSummary, TIndex>()
						.OrderBy(summary => summary.Date)
						.ToArray();

					Assert.Equal(2, results.Length);

					Assert.Equal(2, results[0].Count);
					Assert.Equal(ImportStatus.Complete, results[0].Status);
					Assert.Equal(new DateTime(2013, 1, 1), results[0].Date);

					Assert.Equal(2, results[1].Count);
					Assert.Equal(ImportStatus.Complete, results[1].Status);
					Assert.Equal(new DateTime(2013, 2, 2), results[1].Date);
				}
			}
		}

		// Pass
		public class Client_ImportSummaryByDate_1 : AbstractIndexCreationTask<Client, ImportSummary>
		{
			public Client_ImportSummaryByDate_1()
			{
				Map = clients => clients.SelectMany(x => x.ImportStatuses, (x, y) => new {x.Id, y.Status, y.TimeStamp})
				                        .GroupBy(x => new {x.Id, x.TimeStamp.Date})
				                        .Select(g => g.OrderBy(x => x.TimeStamp).Last())
				                        .Select(x => new
				                        {
					                        x.Status,
					                        x.TimeStamp.Date,
					                        Count = 1
				                        });

				Reduce = results => from result in results
				                    group result by new {result.Status, result.Date}
				                    into g
				                    select new
				                    {
					                    g.Key.Status,
					                    g.Key.Date,
					                    Count = g.Sum(x => x.Count)
				                    };
			}
		}

		// Fail
		public class Client_ImportSummaryByDate_2 : AbstractIndexCreationTask<Client, ImportSummary>
		{
			public Client_ImportSummaryByDate_2()
			{
				Map = clients => clients.SelectMany(x => x.ImportStatuses, (x, y) => new {x.Id, y.Status, y.TimeStamp})
				                        .GroupBy(x => new {x.Id, x.TimeStamp.Date})
				                        .Select(g => new
				                        {
					                        g.OrderBy(x => x.TimeStamp).Last().Status,
					                        g.Key.Date,
					                        Count = 1
				                        });

				Reduce = results => from result in results
				                    group result by new {result.Status, result.Date}
				                    into g
				                    select new
				                    {
					                    g.Key.Status,
					                    g.Key.Date,
					                    Count = g.Sum(x => x.Count)
				                    };

			}
		}

		// Fail
		public class Client_ImportSummaryByDate_3 : AbstractIndexCreationTask<Client, ImportSummary>
		{
			public Client_ImportSummaryByDate_3()
			{
				Map = clients => from client in clients
				                 from status in client.ImportStatuses
				                 group status by new {client.Id, status.TimeStamp.Date}
				                 into g
				                 let z = g.OrderBy(x => x.TimeStamp).Last()
				                 select new
				                 {
					                 z.Status,
					                 g.Key.Date,
					                 Count = 1
				                 };

				Reduce = results => from result in results
				                    group result by new {result.Status, result.Date}
				                    into g
				                    select new
				                    {
					                    g.Key.Status,
					                    g.Key.Date,
					                    Count = g.Sum(x => x.Count)
				                    };

			}
		}

		// Fail
		public class Client_ImportSummaryByDate_4 : AbstractIndexCreationTask<Client, ImportSummary>
		{
			public Client_ImportSummaryByDate_4()
			{
				Map = clients => from client in clients
				                 from status in client.ImportStatuses
				                 group status by new {client.Id, status.TimeStamp.Date}
				                 into g
				                 select new
				                 {
					                 g.OrderBy(x => x.TimeStamp).Last().Status,
					                 g.Key.Date,
					                 Count = 1
				                 };

				Reduce = results => from result in results
				                    group result by new {result.Status, result.Date}
				                    into g
				                    select new
				                    {
					                    g.Key.Status,
					                    g.Key.Date,
					                    Count = g.Sum(x => x.Count)
				                    };

			}
		}

		public class ImportSummary
		{
			public ImportStatus Status { get; set; }
			public DateTime Date { get; set; }
			public int Count { get; set; }
		}

		protected override void CreateDefaultIndexes(IDocumentStore documentStore)
		{
			// No need to create the default index here
		}
	}
}