using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Corax;
using CsvHelper;
using CsvHelper.Configuration;
using MimeKit;
using SharpCompress.Common;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Voron;

namespace Tryouts
{
    class CoraxReddit
    {
        public const string DirectoryEnron = "reddit-corax";

        public static void IndexInCorax(bool recreateDatabase = true, string outputDirectory = DirectoryEnron)
        {
            var path = Path.Join("..", Reddit.DatasetFile);

            if (Directory.Exists(DirectoryEnron))
                Directory.Delete(DirectoryEnron, true);

            var options = StorageEnvironmentOptions.ForPath(outputDirectory);
            var env = new StorageEnvironment(options);

            var sp = Stopwatch.StartNew();
            var indexOnlySp = new Stopwatch();

            var gz = SharpCompress.Readers.GZip.GZipReader.Open(File.OpenRead(path));

            var indexWriter = new IndexWriter(env);

            int i = 0;
            long ms = 0;
            long justIndex = 0;
            gz.MoveToNextEntry();

            using var s = gz.OpenEntryStream();
            var entryReader = new CsvReader(new StreamReader(s), new CsvConfiguration(CultureInfo.InvariantCulture));

            // Ignore the header.
            entryReader.Read();

            var ctx = JsonOperationContext.ShortTermSingleUse();
            while (entryReader.Read())
            {
                entryReader.TryGetField<int>(0, out var id);
                entryReader.TryGetField<string>(1, out var name);
                entryReader.TryGetField<long>(2, out var createdUtc);
                entryReader.TryGetField<long>(3, out var updatedOn);
                entryReader.TryGetField<int>(4, out var commentKarma);
                entryReader.TryGetField<int>(5, out var linkKarma);

                var value = new DynamicJsonValue
                {
                    ["Id"] = id,
                    ["Name"] = name,
                    ["CreatedUtc"] = createdUtc,
                    ["UpdatedOn"] = updatedOn,
                    ["CommentKarma"] = commentKarma,
                    ["LinkKarma"] = linkKarma,
                };

                var entry = ctx.ReadObject(value, $"entry/{i}");

                indexOnlySp.Restart();
                indexWriter.Index($"entry/{i}", entry);
                justIndex += indexOnlySp.ElapsedMilliseconds;

                if (i % 1024 * 512 == 0)
                {
                    ms += sp.ElapsedMilliseconds;
                    Console.WriteLine(sp.ElapsedMilliseconds);

                    sp.Restart();

                    indexWriter.Commit();
                    indexWriter.Dispose();

                    indexWriter = new IndexWriter(env);
                    ctx = JsonOperationContext.ShortTermSingleUse();
                }

                i++;
            }
            

            indexWriter.Commit();

            Console.WriteLine($"Indexing time: {justIndex}");
            Console.WriteLine($"Total execution time: {ms}");

            indexWriter.Dispose();
        }
    }
}
