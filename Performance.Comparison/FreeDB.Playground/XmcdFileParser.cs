using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using ICSharpCode.SharpZipLib.BZip2;
using ICSharpCode.SharpZipLib.Tar;
using XmcdParser;

namespace FreeDB.Playground
{
	public class XmcdFileParser
	{
		private readonly string _path;
		private readonly DisksDestination _insert;

		private int reads, parsed, writtern;
		private readonly BlockingCollection<string> _entries = new BlockingCollection<string>();
		private readonly BlockingCollection<Disk> _disks = new BlockingCollection<Disk>();

		public XmcdFileParser(string path, DisksDestination insert)
		{
			_path = path;
			_insert = insert;
		}

		public void Start()
		{
			var sp = Stopwatch.StartNew();
			var reader = Task.Factory.StartNew(ReadFile);
			var parser = Task.Factory.StartNew(ParseEntries);
			var processer = Task.Factory.StartNew(ProcessDisks);

			while (true)
			{
				var tasks = new[] { reader, parser, processer, Task.Delay(1000) };
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
				Console.Write("\r{0,10:#,#} sec reads: {1,10:#,#} parsed: {2,10:#,#} written: {3,10:#,#}", sp.Elapsed.TotalSeconds, reads,
					parsed, writtern);
			}
			Console.WriteLine();
			Console.WriteLine("Total");
			Console.WriteLine("{0,10:#,#} reads: {1:10:#,#} parsed: {2:10:#,#} written: {3:10:#,#}", sp.ElapsedMilliseconds, reads,
					parsed, writtern);

		}

		private void ProcessDisks()
		{
			while (true)
			{
				var entry = _disks.Take();
				if (entry == null)
					break;
				_insert.Accept(entry);
				Interlocked.Increment(ref writtern);
			}
			_insert.Done();
		}

		private void ParseEntries()
		{
			var parser = new Parser();

			while (true)
			{
				var entry = _entries.Take();
				if (entry == null)
					break;
				var disk = parser.Parse(entry);
				_disks.Add(disk);
				Interlocked.Increment(ref parsed);
			}

			_disks.Add(null);
		}

		private void ReadFile()
		{
			var buffer = new byte[1024 * 1024];// more than big enough for all files

			using (var bz2 = new BZip2InputStream(File.Open(_path, FileMode.Open)))
			using (var tar = new TarInputStream(bz2))
			{
				TarEntry entry;
				while ((entry = tar.GetNextEntry()) != null)
				{
					if (entry.Size == 0 || entry.Name == "README" || entry.Name == "COPYING")
						continue;
					var readSoFar = 0;
					while (true)
					{
						var bytes = tar.Read(buffer, readSoFar, ((int)entry.Size) - readSoFar);
						if (bytes == 0)
							break;

						readSoFar += bytes;
					}
					// we do it in this fashion to have the stream reader detect the BOM / unicode / other stuff
					// so we can reads the values properly
					var fileText = new StreamReader(new MemoryStream(buffer, 0, readSoFar)).ReadToEnd();
					_entries.Add(fileText);
					Interlocked.Increment(ref reads);
				}
			}
			_entries.Add(null);
		}
	}
}