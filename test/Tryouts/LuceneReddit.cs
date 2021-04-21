using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using CsvHelper;
using CsvHelper.Configuration;
using Lucene.Net.Analysis.Standard;
using Lucene.Net.Documents;
using Lucene.Net.Index;
using Lucene.Net.Store;
using MimeKit;

namespace Tryouts
{
    class LuceneReddit
    {
        public const string EnronLucene = "reddit-lucene";


        public static void IndexInLucene(bool recreateDatabase = true, string outputDirectory = EnronLucene)
        {
            var path = Path.Join("..", Reddit.DatasetFile);

            var sp = Stopwatch.StartNew();
            var indexOnlySp = new Stopwatch();

            using var dir = FSDirectory.Open(outputDirectory);
            var analyzer = new StandardAnalyzer(Lucene.Net.Util.LuceneVersion.LUCENE_48);
            var writer = new IndexWriter(dir, new IndexWriterConfig(Lucene.Net.Util.LuceneVersion.LUCENE_48, analyzer));

            int i = 0;
            long ms = 0;
            using var gz = SharpCompress.Readers.GZip.GZipReader.Open(File.OpenRead(path));
            gz.MoveToNextEntry();

            var s = gz.OpenEntryStream();
            var entryReader = new CsvReader(new StreamReader(s), new CsvConfiguration(CultureInfo.InvariantCulture));

            // Ignore the header.
            entryReader.Read();

            while (entryReader.Read())
            {
                if (i == 1000000)
                    break;

                entryReader.TryGetField<int>(0, out var id);
                entryReader.TryGetField<string>(1, out var name);
                entryReader.TryGetField<long>(2, out var createdUtc);
                entryReader.TryGetField<long>(3, out var updatedOn);
                entryReader.TryGetField<int>(4, out var commentKarma);
                entryReader.TryGetField<int>(5, out var linkKarma);

                var doc = new Document
                {
                    new Int32Field("Id", id, Field.Store.YES),
                    new StringField("Name", name, Field.Store.YES),
                    new Int64Field("CreatedUtc", createdUtc, Field.Store.YES),
                    new Int64Field("UpdatedOn", updatedOn, Field.Store.YES),
                    new Int32Field("CommentKarma", commentKarma, Field.Store.YES),
                    new Int32Field("LinkKarma", linkKarma, Field.Store.YES)
                };

                writer.AddDocument(doc);

                if (i % 1024 * 512 == 0)
                {
                    ms += sp.ElapsedMilliseconds;
                    Console.WriteLine(sp.ElapsedMilliseconds);

                    sp.Restart();

                    writer.Commit();
                    writer.Dispose();
                    writer = new IndexWriter(dir, new IndexWriterConfig(Lucene.Net.Util.LuceneVersion.LUCENE_48, analyzer));
                }

                i++;
            }
            

            writer.Flush(true, true);
            writer.Commit();

            Console.WriteLine($"Indexing time: {ms}");
            Console.WriteLine($"Total execution time: {sp.ElapsedMilliseconds}");

            writer.Dispose();
        }
    }
}
