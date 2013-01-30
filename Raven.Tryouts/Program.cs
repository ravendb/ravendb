using System;
using System.Collections.Generic;
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

			var sntpClient = new SntpClient(new[]
			{
				"time.nist.gov",
				"time-nw.nist.gov",
				"time-a.nist.gov",
				"time-b.nist.gov",
				"time-a.timefreq.bldrdoc.gov",
				"time-b.timefreq.bldrdoc.gov",
				"time-c.timefreq.bldrdoc.gov",
				"utcnist.colorado.edu",
				"nist1.datum.com",
				"nist1.dc.certifiedtime.com",
				"nist1.nyc.certifiedtime.com",
			});

			Console.WriteLine(sntpClient.GetDateAsync().Result);
			Console.ReadLine();
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