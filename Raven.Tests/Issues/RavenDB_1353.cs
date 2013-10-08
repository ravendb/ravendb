// -----------------------------------------------------------------------
//  <copyright file="RavenDB_1353.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Client.Embedded;
using Raven.Client.Indexes;
using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_1353 : RavenTest
	{
		class Operation
		{
			public long Id { get; set; }
			public List<OperationUnit> Units { get; set; }
		}

		class OperationUnit
		{
			public string Id { get; set; }
			public string ReportId { get; set; }
			public string Language { get; set; }
		}

		class Report
		{
			public ReportResults Results { get; set; }
			public string Id { get; set; }
		}

		class ReportResults
		{
			public List<SegmentReportEntry> Segments { get; set; }
		}

		class SegmentReportEntry
		{
			public string Source { get; set; }
			public string Target { get; set; }
			public long Id { get; set; }
			public List<ErrorEntry> Errors { get; set; }
		}

		class ErrorEntry
		{
			public int CheckNumber { get; set; }
			public string CheckName { get; set; }
			public string CategoryName { get; set; }
			public string Message { get; set; }
			public string MessageFormat { get; set; }
		}

		class InvalidMapReduceIndexWithTooManyOutputsPerDocument : AbstractIndexCreationTask<Operation, InvalidMapReduceIndexWithTooManyOutputsPerDocument.ReduceResult>
		{
			public class ReduceResult
			{
				public string OperationId { get; set; }
				public long SegmentId { get; set; }
				public string Source { get; set; }
				public string Target { get; set; }
				public string TargetLanguage { get; set; }
				public int CheckNumber { get; set; }
				public string ErrorMessage { get; set; }
				public int Count { get; set; }
			}

			public InvalidMapReduceIndexWithTooManyOutputsPerDocument()
			{
				Map = operations => from operation in operations
									from unit in operation.Units
									let report = LoadDocument<Report>(unit.ReportId)
									from segment in report.Results.Segments
									from error in segment.Errors
									select new
									{
										OperationId = operation.Id,
										SegmentId = segment.Id,
										Source = segment.Source,
										TargetLanguage = unit.Language,
										Target = segment.Target,
										CheckNumber = error.CheckNumber,
										ErrorMessage = error.Message,
										Count = 1
									};

				Reduce = results => from result in results
									group result by new { result.OperationId, result.Source, result.Target, result.TargetLanguage, result.CheckNumber, result.ErrorMessage } into g
									select new
									{
										g.Key.OperationId,
										g.First().SegmentId,
										g.Key.Source,
										g.Key.Target,
										g.Key.TargetLanguage,
										g.Key.CheckNumber,
										g.Key.ErrorMessage,
										Count = g.Sum(x => x.Count)
									};
			}
		}

		[Fact]
		public void ShouldMarkMapReduceIndexAsErroredWhenItProducesTooManyOutputsPerDocument()
		{
			using (var store = NewDocumentStore())
			{
				var index = new InvalidMapReduceIndexWithTooManyOutputsPerDocument();
				index.Execute(store);

				PutDocuments(store);

				WaitForIndexing(store);

				var stats = store.DatabaseCommands.GetStatistics().Indexes.First(x => x.PublicName == index.IndexName);

				Assert.Equal(IndexingPriority.Error, stats.Priority);
			}
		}

		private static void PutDocuments(EmbeddableDocumentStore store)
		{
			using (var session = store.OpenSession())
			{
				var operation1 = new Operation()
				{
					Id = 1,
					Units = new List<OperationUnit>()
					{
						new OperationUnit()
						{
							Id = "unit/1",
							Language = "en",
							ReportId = "reports/1"
						},
						new OperationUnit()
						{
							Id = "unit/2",
							Language = "en",
							ReportId = "reports/2"
						}
					}
				};

				var operation2 = new Operation()
				{
					Id = 2,
					Units = new List<OperationUnit>()
					{
						new OperationUnit()
						{
							Id = "unit/3",
							Language = "en",
							ReportId = "reports/1"
						},
						new OperationUnit()
						{
							Id = "unit/4",
							Language = "en",
							ReportId = "reports/2"
						}
					}
				};

				var report1 = new Report()
				{
					Id = "reports/1",
					Results = new ReportResults()
					{
						Segments = new List<SegmentReportEntry>()
						{
							new SegmentReportEntry()
							{
								Id = 1,
								Target = "a",
								Source = "b",
								Errors = new List<ErrorEntry>()
								{
									new ErrorEntry()
									{
										CategoryName = "a",
										CheckName = "a",
										CheckNumber = 1,
										Message = "msg1",
										MessageFormat = "format1"
									},
									new ErrorEntry()
									{
										CategoryName = "b",
										CheckName = "b",
										CheckNumber = 2,
										Message = "msg2",
										MessageFormat = "format2"
									},
									new ErrorEntry()
									{
										CategoryName = "c",
										CheckName = "c",
										CheckNumber = 3,
										Message = "msg3",
										MessageFormat = "format3"
									},
									new ErrorEntry()
									{
										CategoryName = "d",
										CheckName = "d",
										CheckNumber = 3,
										Message = "msg3",
										MessageFormat = "format3"
									},
									new ErrorEntry()
									{
										CategoryName = "d",
										CheckName = "d",
										CheckNumber = 5,
										Message = "msg3",
										MessageFormat = "format3"
									}
								}
							},
							new SegmentReportEntry()
							{
								Id = 2,
								Target = "a",
								Source = "b",
								Errors = new List<ErrorEntry>()
								{
									new ErrorEntry()
									{
										CategoryName = "a",
										CheckName = "a",
										CheckNumber = 1,
										Message = "msg1",
										MessageFormat = "format1"
									},
									new ErrorEntry()
									{
										CategoryName = "b",
										CheckName = "b",
										CheckNumber = 2,
										Message = "msg2",
										MessageFormat = "format2"
									},
									new ErrorEntry()
									{
										CategoryName = "c",
										CheckName = "c",
										CheckNumber = 3,
										Message = "msg3",
										MessageFormat = "format3"
									},
									new ErrorEntry()
									{
										CategoryName = "f",
										CheckName = "f",
										CheckNumber = 6,
										Message = "msg3",
										MessageFormat = "format3"
									},
									new ErrorEntry()
									{
										CategoryName = "g",
										CheckName = "g",
										CheckNumber = 8,
										Message = "msg3",
										MessageFormat = "format3"
									}
								}
							}
						}
					}
				};

				var report2 = new Report()
				{
					Id = "reports/2",
					Results = new ReportResults()
					{
						Segments = new List<SegmentReportEntry>()
						{
							new SegmentReportEntry()
							{
								Target = "a",
								Source = "b",
								Errors = new List<ErrorEntry>()
								{
									new ErrorEntry()
									{
										CategoryName = "a",
										CheckName = "a",
										CheckNumber = 1,
										Message = "msg1",
										MessageFormat = "format1"
									},
									new ErrorEntry()
									{
										CategoryName = "b",
										CheckName = "b",
										CheckNumber = 2,
										Message = "msg2",
										MessageFormat = "format2"
									},
									new ErrorEntry()
									{
										CategoryName = "c",
										CheckName = "c",
										CheckNumber = 3,
										Message = "msg3",
										MessageFormat = "format3"
									}
								}
							},
							new SegmentReportEntry()
							{
								Target = "a",
								Source = "b",
								Errors = new List<ErrorEntry>()
								{
									new ErrorEntry()
									{
										CategoryName = "a",
										CheckName = "a",
										CheckNumber = 1,
										Message = "msg1",
										MessageFormat = "format1"
									},
									new ErrorEntry()
									{
										CategoryName = "b",
										CheckName = "b",
										CheckNumber = 2,
										Message = "msg2",
										MessageFormat = "format2"
									},
									new ErrorEntry()
									{
										CategoryName = "c",
										CheckName = "c",
										CheckNumber = 3,
										Message = "msg3",
										MessageFormat = "format3"
									}
								}
							}
						}
					}
				};

				session.Store(report1);
				session.Store(report2);
				session.Store(operation1);
				session.Store(operation2);

				session.SaveChanges();
			}
		}
	}
}