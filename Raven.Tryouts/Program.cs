using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using System.Xml.Linq;
using Microsoft.Isam.Esent.Interop;
using Raven.Abstractions.Data;
using Raven.Client;
using Raven.Client.Embedded;
using Raven.Client.FileSystem;
using Raven.Client.Indexes;
using Raven.Json.Linq;
using Raven.Tests.Common;

namespace Raven.Tryouts
{
	public class Program
	{
		private static void Main()
		{
            Console.WriteLine(SystemParameters.BookmarkMost);
		    return;
			var filesStore = new FilesStore
			{
				Url = "http://localhost:8080",
				DefaultFileSystem = "test"
			};
			filesStore.Initialize();
			for (int i = 0; i < 5; i++)
			{
				GetValue(filesStore).Wait();
			}
		}

		private static async Task GetValue(FilesStore filesStore)
		{
			using (var fileSession1 = filesStore.OpenAsyncSession())
			{
				try
				{
					var watch = Stopwatch.StartNew();
					var downloadedStream = await fileSession1.DownloadAsync("/hwip/HWIP_v0.2.AsterixMiniRite312_v0.2");

					Console.WriteLine("Stream acquired - time elapsed (ms): {0}", watch.ElapsedMilliseconds);
					watch.Restart();
					downloadedStream.CopyTo(new MemoryStream());
					Console.WriteLine("Streamed and decompressed the document. Time elapsed (ms): {0}", watch.ElapsedMilliseconds);
				}
				catch (Exception e)
				{
					Console.WriteLine(e);
				}
			}
			Console.WriteLine("Storing done!");
		}
	}
}