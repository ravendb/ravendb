using ICSharpCode.SharpZipLib.BZip2;
using ICSharpCode.SharpZipLib.Tar;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Client.Document;
using Raven.Client.Indexes;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;

namespace Raven.Tryouts
{
    public class Disk
    {
        public string Title { get; set; }
        public string Artist { get; set; }
        public int DiskLength { get; set; }
        public string Genre { get; set; }
        public int Year { get; set; }
        public List<string> DiskIds { get; set; }

        public List<int> TrackFramesOffsets { get; set; }
        public List<string> Tracks { get; set; }
        public Dictionary<string, string> Attributes { get; set; }
        public Disk()
        {
            TrackFramesOffsets = new List<int>();
            Tracks = new List<string>();
            DiskIds = new List<string>();
            Attributes = new Dictionary<string, string>();
        }
    }

    public class Parser
    {
        readonly List<Tuple<Regex, Action<Disk, MatchCollection>>> actions = new List<Tuple<Regex, Action<Disk, MatchCollection>>>();

        public Parser()
        {
            Add(@"^\#\s+xmcd", (disk, collection) =>
            {
                if (collection.Count == 0)
                    throw new InvalidDataException("Not an XMCD file");
            });

            Add(@"^\# \s* Track \s+ frame \s+ offsets \s*: \s* \n (^\# \s* (\d+) \s* \n)+", (disk, collection) =>
            {
                foreach (Capture capture in collection[0].Groups[2].Captures)
                {
                    disk.TrackFramesOffsets.Add(int.Parse(capture.Value));
                }
            });

            Add(@"Disc \s+ length \s*: \s* (\d+)", (disk, collection) =>
                                                              disk.DiskLength = int.Parse(collection[0].Groups[1].Value)
                );

            Add("DISCID=(.+)", (disk, collection) =>
            {
                var strings = collection[0].Groups[1].Value.Split(new[] { "," },
                                                                  StringSplitOptions.RemoveEmptyEntries);
                disk.DiskIds.AddRange(strings.Select(x => x.Trim()));
            });

            Add("DTITLE=(.+)", (disk, collection) =>
            {
                var parts = collection[0].Groups[1].Value.Split(new[] { "/" }, 2, StringSplitOptions.RemoveEmptyEntries);
                if (parts.Length == 2)
                {
                    disk.Artist = parts[0].Trim();
                    disk.Title = parts[1].Trim();
                }
                else
                {
                    disk.Title = parts[0].Trim();
                }
            });

            Add(@"DYEAR=(\d+)", (disk, collection) =>
            {
                if (collection.Count == 0)
                    return;
                var value = collection[0].Groups[1].Value;
                if (value.Length > 4) // there is data like this
                {
                    value = value.Substring(value.Length - 4);
                }
                disk.Year = int.Parse(value);
            }
            );

            Add(@"DGENRE=(.+)", (disk, collection) =>
            {
                if (collection.Count == 0)
                    return;
                disk.Genre = collection[0].Groups[1].Value.Trim();
            }
            );

            Add(@"TTITLE\d+=(.+)", (disk, collection) =>
            {
                foreach (Match match in collection)
                {
                    disk.Tracks.Add(match.Groups[1].Value.Trim());
                }
            });

            Add(@"(EXTD\d*)=(.+)", (disk, collection) =>
            {
                foreach (Match match in collection)
                {
                    var key = match.Groups[1].Value;
                    string value;
                    if (disk.Attributes.TryGetValue(key, out value))
                    {
                        disk.Attributes[key] = value + match.Groups[2].Value.Trim();
                    }
                    else
                    {
                        disk.Attributes[key] = match.Groups[2].Value.Trim();
                    }
                }
            });
        }

        private void Add(string regex, Action<Disk, MatchCollection> action)
        {
            var key = new Regex(regex, RegexOptions.Compiled | RegexOptions.IgnoreCase | RegexOptions.IgnorePatternWhitespace | RegexOptions.Multiline);
            actions.Add(Tuple.Create(key, action));
        }

        public Disk Parse(string text)
        {
            var disk = new Disk();
            foreach (var action in actions)
            {
                var collection = action.Item1.Matches(text);
                try
                {
                    action.Item2(disk, collection);
                }
                catch (Exception e)
                {
                    Console.WriteLine();
                    Console.WriteLine(text);
                    Console.WriteLine(action.Item1);
                    Console.WriteLine(e);
                    throw;
                }
            }

            return disk;
        }
    }

    public class DisksSearch : AbstractIndexCreationTask
    {
        public override string IndexName
        {
            get
            {
                return "Disks/Search";
            }
        }
        public override IndexDefinition CreateIndexDefinition()
        {
            return new IndexDefinition
            {
                Map = @" from disk in docs.Disks
                         select new
                         {
                             Query = new[] 
                             {
                                 disk.Artist,
                                 disk.Title
                             },
                             disk.Tracks,
                             DiskId = disk.DiskIds
                         }",
                Indexes =  
                {
				    {
					    "Query",
					    FieldIndexing.Analyzed
				    },
				    {
					    "Tracks",
					    FieldIndexing.Analyzed
				    }
			    },
                Analyzers =  
                {
				    {
					    "Query", "StandardAnalyzer"
				    },
				    {
					    "Tracks", "StandardAnalyzer"
				    }
			    }
            };
        }
    }

    public class BulkInsertsPerformance
    {
        private class Data
        {
            public string Method;
            public TimeSpan Elapsed;            
        }

        private const int BatchSize = 24;
        static void Main()
        {
            var list = new List<Data>();

            //list.Add(new Data
            //{
            //    Method = "Bson/None",
            //    Elapsed = Benchmark("database-1a", new BulkInsertOptions { Format = BulkInsertFormat.Bson, Compression = BulkInsertCompression.None })
            //});

            list.Add(new Data
            {
                Method = "Json/None",
                Elapsed = Benchmark("database-2a", new BulkInsertOptions { Format = BulkInsertFormat.Json, Compression = BulkInsertCompression.None })
            });

            //list.Add(new Data
            //{
            //    Method = "Bson/GZip",
            //    Elapsed = Benchmark("database-3c", new BulkInsertOptions { Format = BulkInsertFormat.Bson, Compression = BulkInsertCompression.GZip })
            //});

            //list.Add(new Data
            //{
            //    Method = "Json/GZip",
            //    Elapsed = Benchmark("database-4a", new BulkInsertOptions { Format = BulkInsertFormat.Json, Compression = BulkInsertCompression.GZip })
            //});

            foreach ( var data in list )
                Console.WriteLine(string.Format( "Method: {0} - {1}." , data.Method, data.Elapsed));

            Console.ReadLine();
        }

        public static TimeSpan Benchmark(string database, BulkInsertOptions options)
        {            
            using (var store = new DocumentStore
            {
                Url = "http://localhost:8080",
                DefaultDatabase = database,
            }.Initialize(true))
            {
                IndexCreation.CreateIndexes(typeof(Program).GetType().Assembly, store);

                var sp = Stopwatch.StartNew();
                using (var insert = store.BulkInsert(options: options))
                {
                    insert.Report += Console.WriteLine;
                    ParseDisks(insert);
                }

                while (store.DatabaseCommands.GetStatistics().StaleIndexes.Length != 0)
                    Thread.Sleep(500);

                return sp.Elapsed;
            }
        }

        private static void ParseDisks(BulkInsertOperation insert)
        {
            int i = 0;
            var parser = new Parser();
            var buffer = new byte[1024 * 1024];// more than big enough for all files

            using (var bz2 = new BZip2InputStream(File.Open(@"I:\Temp\freedb-complete-20150101.tar.bz2", FileMode.Open)))
            using (var tar = new TarInputStream(bz2))
            {
                int processed = 0;

                TarEntry entry;
                while ((entry = tar.GetNextEntry()) != null)
                {
                    if (processed >= 1000000)
                        return;

                    if (entry.Size == 0 || entry.Name == "README" || entry.Name == "COPYING")
                        continue;

                    var readSoFar = 0;
                    while (true)
                    {
                        var read = tar.Read(buffer, readSoFar, ((int)entry.Size) - readSoFar);
                        if (read == 0)
                            break;

                        readSoFar += read;
                    }

                    // we do it in this fashion to have the stream reader detect the BOM / unicode / other stuff
                    // so we can read the values properly
                    var fileText = new StreamReader(new MemoryStream(buffer, 0, readSoFar)).ReadToEnd();
                    try
                    {
                        var disk = parser.Parse(fileText);
                        insert.Store(disk);

                        processed++;
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine();
                        Console.WriteLine(entry.Name);
                        Console.WriteLine(e);
                    }
                }
            }
        }
    }	
}
