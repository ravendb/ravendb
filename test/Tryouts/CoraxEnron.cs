using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using Corax;
using MimeKit;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Voron;

namespace Tryouts
{
    class CoraxEnron
    {
        public const string DirectoryEnron = "enron-corax";

        public static void IndexInCorax(bool recreateDatabase = true, string outputDirectory = DirectoryEnron)
        {
            var path = Path.Join("..", Enron.DatasetFile);

            if (Directory.Exists(DirectoryEnron))
                Directory.Delete(DirectoryEnron, true);

            using var options = StorageEnvironmentOptions.ForPath(outputDirectory);
            using var env = new StorageEnvironment(options);

            var sp = Stopwatch.StartNew();
            var indexOnlySp = new Stopwatch();

            using var tar = SharpCompress.Readers.Tar.TarReader.Open(File.OpenRead(path));

            var indexWriter = new IndexWriter(env);

            int i = 0;
            long ms = 0;
            long justIndex = 0;
            using var ctx = JsonOperationContext.ShortTermSingleUse();
            while (tar.MoveToNextEntry())
            {
                if (tar.Entry.IsDirectory)
                    continue;

                using var s = tar.OpenEntryStream();
                var msg = MimeMessage.Load(s);

                var value = new DynamicJsonValue
                {
                    ["Bcc"] = (msg.Bcc ?? Enumerable.Empty<InternetAddress>()).Select(x => x.ToString()).ToArray(),
                    ["Cc"] = (msg.Cc ?? Enumerable.Empty<InternetAddress>()).Select(x => x.ToString()).ToArray(),
                    ["To"] = (msg.To ?? Enumerable.Empty<InternetAddress>()).Select(x => x.ToString()).ToArray(),
                    ["From"] = msg.From?.FirstOrDefault()?.ToString(),
                    ["ReplyTo"] = msg.ReplyTo?.FirstOrDefault()?.ToString(),
                    ["Body"] = msg.GetTextBody(MimeKit.Text.TextFormat.Plain).Split(' '),
                    ["References"] = (msg.References ?? Enumerable.Empty<string>()).ToArray(),
                    ["Subject"] = msg.Subject.Split(' '),
                    ["MessageId"] = msg.MessageId,
                    ["Date"] = msg.Date.ToString("O"),
                    ["Importance"] = msg.Importance.ToString(),
                    ["Priority"] = msg.Priority.ToString(),
                };

                foreach (var item in msg.Headers ?? new HeaderList())
                {
                    if (item.Value.Length > 512)
                        continue;

                    string headerName = item.Id.ToHeaderName();
                    if (headerName.Length < 128)
                        value[headerName] = item.Value;
                }

                if (msg.Sender != null)
                    value["Sender"] = msg.Sender.ToString();

                if (msg.InReplyTo != null)
                    value["InReplyTo"] = msg.InReplyTo;

                var entry = ctx.ReadObject(value, $"entry/{i}");

                indexOnlySp.Restart();
                indexWriter.Index($"entry/{i}", entry);
                justIndex += indexOnlySp.ElapsedMilliseconds;

                if (i % 1024 * 16 == 0)
                {
                    ms += sp.ElapsedMilliseconds;
                    Console.WriteLine(sp.ElapsedMilliseconds);

                    sp.Restart();

                    indexWriter.Commit();
                    indexWriter.Dispose();
                    
                    indexWriter = new IndexWriter(env);
                    ctx.Reset();
                }

                i++;
            }

            indexWriter.Commit();
            indexWriter.Dispose();

            Console.WriteLine($"Indexing time: {justIndex}");
            Console.WriteLine($"Total execution time: {ms}");
        }
    }
}
