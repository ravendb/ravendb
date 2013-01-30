using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using Lucene.Net.Analysis.Tokenattributes;
using Lucene.Net.Spatial.Prefix.Tree;
using Raven.Abstractions.Exceptions;
using Raven.Client.Document;
using Raven.Database.Indexing.Spatial;
using Raven.Json.Linq;
using Rhino.Licensing;
using Spatial4n.Core.Context.Nts;
using Spatial4n.Core.Io;
using System.Linq;

namespace BulkStressTest
{
	class Program
	{
		private static void Main()
		{
			//Repro();
			var ctx = new NtsSpatialContext(true);
			var startegy = new RecursivePrefixTreeStrategyThatSupportsWithin(new QuadPrefixTree(ctx, 9), "test");
			var poly = @"POLYGON ((-110.947642 55.333333, -109.644025 55.333333, -109.373584 55.253002, -109.132938 55.146591, -108.929565 55.017525, -108.769563 54.869919, -108.657484 54.708433, -108.596239 54.538106, -108.587078 54.364188, -108.629626 54.191974, -108.721976 54.026641, -108.860817 53.873103, -109.041589 53.735872, -109.258661 53.618939, -109.505505 53.525678, -109.77489 53.45876, -110.059063 53.420091, -110.34994 53.410769, -110.639287 53.431058, -110.918901 53.480383, -111.180801 53.557342, -111.417406 53.659739, -111.621724 53.784634, -111.787539 53.928413, -111.909595 54.086879, -111.983776 54.255356, -112.007276 54.428817, -111.978744 54.602021, -111.898399 54.769672, -111.768106 54.926578, -111.591391 55.067822, -111.373396 55.188926, -111.120753 55.286017, -110.947642 55.333333))";

			var quadPrefixTree = new QuadPrefixTree(ctx);
			Node quadCell = new QuadPrefixTree.QuadCell("", quadPrefixTree);
			for (int i = 0; i < 50; i++)
			{
				Console.WriteLine("{0,-4}: {1:#,#.#####} meters", i, quadCell.GetShape().GetArea(ctx) * 1000);
				quadCell = quadCell.GetSubCells().First();
			}
		}

		private static string NormalizeLineEnding(string script)
		{
			var sb = new StringBuilder();
			using (var reader = new StringReader(script))
			{
				while (true)
				{
					var line = reader.ReadLine();
					if (line == null)
						return sb.ToString();
					sb.AppendLine(line);
				}
			}
		}

		private static void Repro()
		{
			using (var store = new DocumentStore
			{
				Url = "http://localhost:8080",
				DefaultDatabase = "hello"
			}.Initialize())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new ReadingList
					{
						UserId = "test/1",
						Id = "lists/1",
						Books = new List<ReadingList.ReadBook>()
					});
					session.SaveChanges();
				}
				Parallel.For(0, 100, i =>
				{
					while (true)
					{
						try
						{
							using (var session = store.OpenSession())
							{
								session.Advanced.UseOptimisticConcurrency = true;
								session.Load<ReadingList>("lists/1")
									.Books.Add(new ReadingList.ReadBook
									{
										ReadAt = DateTime.Now,
										Title = "test " + i
									});
								session.SaveChanges();
							}
							break;
						}
						catch (ConcurrencyException)
						{
						}
					}
				});
			}
		}


		public class ReadingList
		{
			public string Id { get; set; }
			public string UserId { get; set; }

			public List<ReadBook> Books { get; set; }

			public class ReadBook
			{
				public string Title { get; set; }
				public DateTime ReadAt { get; set; }
			}
		}
	}
}