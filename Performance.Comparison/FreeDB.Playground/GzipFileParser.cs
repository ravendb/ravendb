using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace FreeDB.Playground
{
	public class GzipFileParser
	{
		private readonly string _path;
		private readonly EntryDestination _insert;

		private readonly BlockingCollection<string> _entries = new BlockingCollection<string>(10 * 1000);
		private int _reads, _written, _entriesWrittern;

		public GzipFileParser(string path, EntryDestination insert)
		{
			_path = path;
			_insert = insert;
		}

		public void Start()
		{
			var sp = Stopwatch.StartNew();
			var reader = Task.Factory.StartNew(ReadFile);
			var writer = Task.Factory.StartNew(Writer);

			while (true)
			{
				var tasks = new[] { reader, writer, Task.Delay(1000) };
				var array = tasks
					.Where(x => x.IsCompleted == false)
					.ToArray();
				if (tasks.Any(x => x.IsFaulted))
				{
					tasks.First(x => x.IsFaulted).Wait();
				}
				if (array.Length <= 1)
					break;
				Task.WaitAny(array);
				Console.Write("\r{0,10:#,#} sec r/w/e: {1:#,#}/{2:#,#}/{3:#,#}", sp.Elapsed.TotalSeconds, _reads, _written, _entriesWrittern);
			}
			Console.WriteLine();
			Console.WriteLine("Total");
			Console.Write("\r{0,10:#,#} sec r/w/e: {1:#,#}/{2:#,#}/{3:#,#}", sp.Elapsed.TotalSeconds, _reads, _written, _entriesWrittern);

		}

		private void Writer()
		{
			while (true)
			{
				var entry = _entries.Take();
				if (entry == null)
					break;

				var acc = _insert.Accept(entry);
				Interlocked.Add(ref _entriesWrittern, acc);
				Interlocked.Increment(ref _written);
			}
			_insert.Done();
		}

		private void ReadFile()
		{
			using (var file = File.Open(_path, FileMode.Open))
			using (var stream = new GZipStream(file, CompressionMode.Decompress))
			using (var reader = new StreamReader(stream))
			{
				while (true)
				{
					var readLine = reader.ReadLine();
					if (readLine == null)
						break;
					Interlocked.Increment(ref _reads);
					_entries.Add(readLine);
				}
			}
			_entries.Add(null);
		}


	}
}