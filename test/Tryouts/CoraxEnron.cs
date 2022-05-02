using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using Corax;
using Corax.Queries;
using MimeKit;
using MimeKit.Text;
using Sparrow;
using Sparrow.Server;
using Sparrow.Threading;
using Voron;
using Voron.Data.Containers;
using Voron.Data.Sets;
using Voron.Impl;
using Constants = Voron.Global.Constants;

namespace Tryouts
{

    class CoraxEnron
    {
        public const string DirectoryEnron = @"C:\Work\ravendb-5.1\test\Tryouts\bin\release\net5.0\enron-corax";
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

        private static void ReportStats(StorageEnvironment env)
        {
            //using var rtx = env.ReadTransaction();
            //var reports = env.GenerateReport(rtx);

            //long  big = 0L;
            //var tree = rtx.ReadTree("Fields");
            //long treeSize = tree.State.BranchPages * Constants.Storage.PageSize + tree.State.LeafPages * Constants.Storage.PageSize;

            //long numberOfTerms = 0, numberOfTermValues = 0;
            //using var it = tree.Iterate(false);
            //if(it.Seek(Slices.BeforeAllKeys))
            //{
            //    do
            //    {
            //        var fieldTree = tree.CompactTreeFor(it.CurrentKey);
            //        treeSize += fieldTree.State.BranchPages * Constants.Storage.PageSize + fieldTree.State.LeafPages * Constants.Storage.PageSize;
            //        fieldTree.Seek("\0");
            //        while (fieldTree.Next(out _, out var l))
            //        {
            //            if ((l & (long)IndexWriter.TermIdMask.Set) != 0)
            //            {
            //                numberOfTerms++;
            //                var setSpace = Container.Get(rtx.LowLevelTransaction, (l & ~0b11));
            //                ref var setState = ref MemoryMarshal.GetReference<SetState>(MemoryMarshal.Cast<byte,SetState>(setSpace));
            //                Set set = new Set(rtx.LowLevelTransaction, Slices.Empty, setState);
            //                using var sit = set.Iterate();
            //                if (sit.Seek(0))
            //                {
            //                    do
            //                    {
            //                        numberOfTermValues++;
            //                    } while (sit.MoveNext());
            //                }
            //                big +=  setState.BranchPages * Constants.Storage.PageSize + setState.LeafPages * Constants.Storage.PageSize;
            //            }
            //        }
            //    } while (it.MoveNext());
            //}
         

            //Console.WriteLine($"Total Tree Size: {treeSize:##,###}");
            //Console.WriteLine($"Number of terms: {numberOfTerms:##,###} - values {numberOfTermValues:##,###}");
            //Console.WriteLine($"Big: {big:##,###}");
            //Console.WriteLine("PostingLists:");
            //OutputContainerStats(rtx, IndexWriter.PostingListsSlice);
            //Console.WriteLine("Entries:");
            //OutputContainerStats(rtx, IndexWriter.EntriesContainerSlice);
        }

        private static void OutputContainerStats(Transaction rtx, Slice key)
        {
            var exists = rtx.LowLevelTransaction.RootObjects.Read(key);
            long containerId = exists.Reader.ReadLittleEndianInt64();
            var ids = Container.GetAllIds(rtx.LowLevelTransaction, containerId);
            var dic = new Dictionary<int, (int, int)>();
            Console.WriteLine($"Total {ids.Count:##,###} items in total");
            long size = 0;
            foreach (long id in ids)
            {
                var item = Container.Get(rtx.LowLevelTransaction, id);
                var span = item.ToSpan();
                size += span.Length;
                dic.TryGetValue(span.Length, out var counts);
                dic[span.Length] = (counts.Item1 + span.Length, counts.Item2 + 1);
            }

            Console.WriteLine($"Total size: {size:##,###}");
            // Console.WriteLine($"ItemSize\tTotalItemSize\tNumberOfItems");
            // foreach (var (itemSize, counts) in dic.OrderBy(x => x.Key))
            // {
            //     Console.WriteLine($"{itemSize:##,###}\t{counts.Item1:##,###}\t{counts.Item2:##,###}");
            // }
        }

        public static void Index(bool recreateDatabase = true, string outputDirectory = ".")
        {
            var path =  Enron.DatasetFile;

            string storagePath = DirectoryEnron;
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
                    var knownFields = CreateKnownFields(bsc);

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
                        catch (Exception)
                        {
                            Console.WriteLine(id);
                            throw;
                        }

                        // var entryReader = new IndexEntryReader(data);


                        if (i % (1024*32) == 0)
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
                    
                    //ReportStats(env);
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

        public static void Search(string field, string term)
        {
            var options = StorageEnvironmentOptions.ForPath(DirectoryEnron);
            var env = new StorageEnvironment(options);

            using (var searcher = new IndexSearcher(env))
            {
                Span<long> ids = stackalloc long[128];

                var q = searcher.TermQuery(field, term);
                int read;
                do
                {
                    read = q.Fill(ids);
                    for (int i = 0; i < read; i++)
                        Console.WriteLine(searcher.GetIdentityFor(ids[i]));
                }
                while (read != 0);
            }
        }
        
        private readonly struct StringArrayIterator : IReadOnlySpanIndexer
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

            public bool IsNull(int i)
            {
                if (i < 0 || i >= Length)
                    throw new ArgumentOutOfRangeException();

                return _values[i] == null;
            }

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

        private static IndexFieldsMapping CreateKnownFields(ByteStringContext ctx)
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

            return new IndexFieldsMapping(ctx)
                        .AddBinding(IdIndex, idSlice)
                        .AddBinding(BccIndex, bccSlice)
                        .AddBinding(CcIndex, ccSlice)
                        .AddBinding(ToIndex, toSlice)
                        .AddBinding(FromIndex, fromSlice)
                        .AddBinding(ReplyToIndex, replyToSlice)
                        .AddBinding(BodyIndex, bodySlice)
                        .AddBinding(RefIndex, refSlice)
                        .AddBinding(SubjectIndex, subjectSlice)
                        .AddBinding(MessageIdIndex, messageIdSlice)
                        .AddBinding(DateIndex, dateSlice)
                        .AddBinding(ImportanceIndex, importanceSlice)
                        .AddBinding(PriorityIndex, prioritySlice)
                        .AddBinding(SenderIndex, senderSlice)
                        .AddBinding(InReplyToIndex, inReplyToSlice);
        }
    }
}
