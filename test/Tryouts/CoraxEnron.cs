using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using Corax;
using MimeKit;
using MimeKit.Text;
using Sparrow;
using Sparrow.Json;
using Sparrow.Server;
using Sparrow.Threading;
using Voron;

namespace Tryouts
{

    class CoraxEnron
    {
        public const string DirectoryEnron = "enron-corax";
        private static char[] trimChars = {' ', ',', '\t', '\n'};

        private static IEnumerable<string> NormalizeEmails(IEnumerable<string> emails)
        {
            foreach (var email in emails)
            {
                if (email.Contains(','))
                {
                    foreach (var nEmail in email.Split(','))
                        yield return nEmail.Trim(trimChars);
                }
                else
                    yield return email.Trim(trimChars);
            }
        }

        public class IndexEntry
        {
            public string Id;
            public string From;
            public string ReplyTo;

            public IEnumerable<string> Bcc;
            public IEnumerable<string> Cc;
            public IEnumerable<string> To;
            public IEnumerable<string> Body;
            public IEnumerable<string> Subject;
            public IEnumerable<string> Reference;

            public string MessageId;
            public string Date;
            public string Importance;
            public string Priority;
            public string Sender;
            public string InReplyTo;
        }

        public static void Index(bool recreateDatabase = true, string outputDirectory = ".")
        {
            var path =  Enron.DatasetFile;

            string storagePath = Path.Join(outputDirectory, DirectoryEnron);
            if (Directory.Exists(storagePath))
                Directory.Delete(storagePath, true);

            using var options = StorageEnvironmentOptions.ForPath(storagePath);
            using var env = new StorageEnvironment(options);
            try
            {

                var sp = Stopwatch.StartNew();
                var indexOnlySp = new Stopwatch();

                using var tar = SharpCompress.Readers.Tar.TarReader.Open(File.OpenRead(path));

                var indexWriter = new IndexWriter(env);
                try
                {
                    const int bufferSize = 5200000 * 4;
                    indexWriter.Transaction.Allocator.Allocate(bufferSize, out var buffer);

                    int i = 0;
                    long ms = 0;
                    long justIndex = 0;

                    var bufferSpan = buffer.ToSpan();

                    using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
                    Dictionary<Slice, int> knownFields = CreateKnownFields(bsc);

                    while (tar.MoveToNextEntry())
                    {
                        if (tar.Entry.IsDirectory)
                            continue;

                        using var s = tar.OpenEntryStream();
                        var msg = MimeMessage.Load(s);

                        string[] strings = msg.GetTextBody(TextFormat.Plain).Split(trimChars);
                        for (int j = 0; j < strings.Length; j++)
                        {
                            if (strings[j].Length > 512)
                                strings[j] = strings[j].Substring(0, 512);
                        }

                        var value = new IndexEntry
                        {
                            Bcc = NormalizeEmails((msg.Bcc ?? Enumerable.Empty<InternetAddress>()).Select(x => x.ToString())),
                            Cc = NormalizeEmails((msg.Cc ?? Enumerable.Empty<InternetAddress>()).Select(x => x.ToString())),
                            To = NormalizeEmails((msg.To ?? Enumerable.Empty<InternetAddress>()).Select(x => x.ToString())),
                            From = msg.From?.FirstOrDefault()?.ToString(),
                            ReplyTo = msg.ReplyTo?.FirstOrDefault()?.ToString(),
                            Body = strings,
                            Reference = (msg.References ?? Enumerable.Empty<string>()).ToArray(),
                            Subject = msg.Subject.Split(' '),
                            MessageId = msg.MessageId,
                            Date = msg.Date.ToString("O"),
                            Importance = msg.Importance.ToString(),
                            Priority = msg.Priority.ToString(),
                        };

                        if (msg.Sender != null)
                            value.Sender = msg.Sender.ToString();

                        if (msg.InReplyTo != null)
                            value.InReplyTo = msg.InReplyTo;

                        //var entry = ctx.ReadObject(value, $"entry/{i}");
                        var entryWriter = new IndexEntryWriter(bufferSpan, knownFields);

                        i++;
                        var id = $"entry/{i}";
                        try
                        {

                            var data = CreateIndexEntry(ref entryWriter, value, id);

                            indexOnlySp.Restart();
                            indexWriter.Index(id, data, knownFields);
                            justIndex += indexOnlySp.ElapsedMilliseconds;
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine(id);
                            throw;
                        }

                        // var entryReader = new IndexEntryReader(data);


                        if (i % 1024 == 0)
                        {
                            ms += sp.ElapsedMilliseconds;
                            Console.WriteLine($"Elapsed: {sp.ElapsedMilliseconds} total: {i:##,###}");

                            sp.Restart();
                            indexWriter.Commit();
                            indexWriter.Dispose();

                            indexWriter = new IndexWriter(env);
                            indexWriter.Transaction.Allocator.Allocate(bufferSize, out buffer);
                            bufferSpan = buffer.ToSpan();
                        }
                    }


                    indexWriter.Commit();
                    Console.WriteLine($"Indexing time: {justIndex}");
                    Console.WriteLine($"Total execution time: {ms}");
                }
                finally
                {
                    indexWriter.Dispose();
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
            }
        }

        private readonly struct StringArrayIterator : IReadOnlySpanEnumerator
        {
            private readonly string[] _values;

            private static string[] Empty = new string[0];

            public StringArrayIterator(string[] values)
            {
                _values = values ?? Empty;
            }

            public StringArrayIterator(IEnumerable<string> values)
            {
                _values = values?.ToArray() ?? Empty;
            }

            public int Length => _values.Length;

            public ReadOnlySpan<byte> this[int i] => Encoding.UTF8.GetBytes(_values[i]);
        }

        private static Span<byte> CreateIndexEntry(ref IndexEntryWriter entryWriter, IndexEntry value, string id)
        {
            Span<byte> PrepareString(string value)
            {
                if ( value == null )
                    return Span<byte>.Empty;
                return Encoding.UTF8.GetBytes(value);
            }

            entryWriter.Write(IdIndex, PrepareString(id));
            entryWriter.Write(BccIndex, new StringArrayIterator(value.Bcc));
            entryWriter.Write(CcIndex, new StringArrayIterator(value.Cc));
            entryWriter.Write(ToIndex, new StringArrayIterator(value.To));
            entryWriter.Write(BodyIndex, new StringArrayIterator(value.Body));
            entryWriter.Write(SubjectIndex, new StringArrayIterator(value.Subject));
            entryWriter.Write(RefIndex, new StringArrayIterator(value.Reference));
            entryWriter.Write(FromIndex, PrepareString(value.From));
            entryWriter.Write(ReplyToIndex, PrepareString(value.ReplyTo));
            entryWriter.Write(MessageIdIndex, PrepareString(value.MessageId));
            entryWriter.Write(DateIndex, PrepareString(value.Date));
            entryWriter.Write(ImportanceIndex, PrepareString(value.Importance));
            entryWriter.Write(PriorityIndex, PrepareString(value.Priority));
            entryWriter.Write(SenderIndex, PrepareString(value.Sender));
            entryWriter.Write(InReplyToIndex, PrepareString(value.InReplyTo));

            entryWriter.Finish(out var output);
            return output;
        }

        public const int IdIndex = 0,
            BccIndex = 1,
            CcIndex = 2,
            ToIndex = 3,
            FromIndex = 4,
            ReplyToIndex = 5,
            BodyIndex = 6,
            RefIndex = 7,
            SubjectIndex = 8,
            MessageIdIndex = 9,
            DateIndex = 10,
            ImportanceIndex = 11,
            PriorityIndex = 12,
            SenderIndex = 13,
            InReplyToIndex = 14;

        private static Dictionary<Slice, int> CreateKnownFields(ByteStringContext ctx)
        {
            Slice.From(ctx, "Id", ByteStringType.Immutable, out Slice idSlice);
            Slice.From(ctx, "Bcc", ByteStringType.Immutable, out Slice bccSlice);
            Slice.From(ctx, "Cc", ByteStringType.Immutable, out Slice ccSlice);
            Slice.From(ctx, "To", ByteStringType.Immutable, out Slice toSlice);
            Slice.From(ctx, "From", ByteStringType.Immutable, out Slice fromSlice);
            Slice.From(ctx, "ReplyTo", ByteStringType.Immutable, out Slice replyToSlice);
            Slice.From(ctx, "Body", ByteStringType.Immutable, out Slice bodySlice);
            Slice.From(ctx, "References", ByteStringType.Immutable, out Slice refSlice);
            Slice.From(ctx, "Subject", ByteStringType.Immutable, out Slice subjectSlice);
            Slice.From(ctx, "MessageId", ByteStringType.Immutable, out Slice messageIdSlice);
            Slice.From(ctx, "Date", ByteStringType.Immutable, out Slice dateSlice);
            Slice.From(ctx, "Importance", ByteStringType.Immutable, out Slice importanceSlice);
            Slice.From(ctx, "Priority", ByteStringType.Immutable, out Slice prioritySlice);
            Slice.From(ctx, "Sender", ByteStringType.Immutable, out Slice senderSlice);
            Slice.From(ctx, "InReplyTo", ByteStringType.Immutable, out Slice inReplyToSlice);

            return new Dictionary<Slice, int>
            {
                [idSlice] = IdIndex,
                [bccSlice] = BccIndex,
                [ccSlice] = CcIndex,
                [toSlice] = ToIndex,
                [fromSlice] = FromIndex,
                [replyToSlice] = ReplyToIndex,
                [bodySlice] = BodyIndex,
                [refSlice] = RefIndex,
                [subjectSlice] = SubjectIndex,
                [messageIdSlice] = MessageIdIndex,
                [dateSlice] = DateIndex,
                [importanceSlice] = ImportanceIndex,
                [prioritySlice] = PriorityIndex,
                [senderSlice] = SenderIndex,
                [inReplyToSlice] = InReplyToIndex,
            };
        }
    }
}
