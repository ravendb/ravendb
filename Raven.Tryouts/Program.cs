using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using Raven.Abstractions.Exceptions;
using Raven.Client.Document;
using Raven.Json.Linq;
using Rhino.Licensing;

namespace BulkStressTest
{
	class Program
	{
		private static void Main()
		{
			//Repro();

			var normalizeLineEnding = NormalizeLineEnding("test\rtest\ntest\r\ntest");
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