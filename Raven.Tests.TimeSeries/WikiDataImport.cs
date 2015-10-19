// -----------------------------------------------------------------------
//  <copyright file="WikiDataImport.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.IO.Compression;
using System.Net;
using Raven.Abstractions.TimeSeries;
using Raven.Database.Config;
using Raven.Database.TimeSeries;
using Voron;
using Xunit;

namespace Raven.Tests.TimeSeries
{
	public class WikiDataImport
	{
		[Fact(Skip = "Run only when needed")]
		public void Import()
		{
			var storage = new TimeSeriesStorage("http://localhost:8080/", "TimeSeriesTest", new RavenConfiguration
			{
				TimeSeries =
				{
					DataDirectory = @"C:\Data\TimeSeries\WikiData"
				}
			});

			Console.WriteLine("running");
			var sp = Stopwatch.StartNew();
			ImportWikipedia(storage);
			sp.Stop();
			Console.WriteLine(sp.Elapsed);
		}

		private static void ImportWikipedia(TimeSeriesStorage storage)
		{
			var dir = @"E:\TimeSeries\20150401\Compressed";
			var files = Directory.GetFiles(dir, "pagecounts-*.gz", SearchOption.TopDirectoryOnly);
			for (var fileIndex = 0; fileIndex < files.Length; fileIndex++)
			{
				Console.WriteLine("Importing " + fileIndex);
				if (fileIndex < 70)
				{
					continue;

					using (var writer = storage.CreateWriter())
					{
						writer.CreateType("Wiki", new[] { "Views", "Size" });
						writer.Commit();
					}
				}

				if (fileIndex > 100)
					break;

				var fileName = files[fileIndex];
				var path = fileName;

				using (var stream = File.OpenRead(path))
				using (var uncompressed = new GZipStream(stream, CompressionMode.Decompress))
				{
					var lines = 0;
					var writer = storage.CreateWriter();
					try
					{
						using (var reader = new StreamReader(uncompressed))
						{
							string line;
							while ((line = reader.ReadLine()) != null)
							{
								if (string.IsNullOrEmpty(line))
									continue;

								var items = line.Split(new[] {' '}, StringSplitOptions.RemoveEmptyEntries);

								if (items.Length < 4)
									continue;

								var entryName = items[0] + "/" + WebUtility.UrlDecode(items[1]);
								if (entryName.Length > 512)
									continue;

								var time = DateTime.ParseExact(fileName.Replace(@"E:\TimeSeries\20150401\Compressed\pagecounts-", "").Replace(".gz", ""), "yyyyMMdd-HHmmss", CultureInfo.InvariantCulture);
								var views = long.Parse(items[2]);
								var size = long.Parse(items[3]);
								writer.Append("Wiki", entryName, time, views, size);

								if (lines++%1000 == 0)
								{
									writer.Commit();
									writer.Dispose();
									writer = storage.CreateWriter();
								}
							}
						}
					}
					finally
					{
						writer.Commit();
						writer.Dispose();
					}
				}
			}
		}
	}
}