using System.IO;
using System.Text;

using Voron.Debugging;
using Voron.Impl;
using Xunit;

namespace Voron.Tests
{
    public class DebugJournalTest
    {
        private const string debugJouralName = "debug_journal_test";

        public DebugJournalTest()
        {
            const string fileNameWithDebugJournalExtension = debugJouralName + ".djrs";

            if (File.Exists(fileNameWithDebugJournalExtension))
                File.Delete(fileNameWithDebugJournalExtension);
        }

        [PrefixesFact]
        public void Record_debug_journal_and_replay_it()
        {
            var structSchema = new StructureSchema<SampleStruct>()
                    .Add<int>(SampleStruct.Foo)
                    .Add<string>(SampleStruct.Bar);

            using (var env = new StorageEnvironment(StorageEnvironmentOptions.CreateMemoryOnly()))
            {
                env.DebugJournal = new DebugJournal(debugJouralName, env, true);
                using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
                {
                    env.CreateTree(tx, "test-tree");
                    tx.Commit();
                }

                using (var writeBatch = new WriteBatch())
                {
                    var valueBuffer = new MemoryStream(Encoding.UTF8.GetBytes("{ \"title\": \"foo\",\"name\":\"bar\"}"));
                    writeBatch.Add("foo", valueBuffer, "test-tree");

                    valueBuffer = new MemoryStream(Encoding.UTF8.GetBytes("testing testing 1 2!"));
                    writeBatch.Add("bar", valueBuffer, "test-tree");

                    valueBuffer = new MemoryStream(Encoding.UTF8.GetBytes("testing testing 1 2 3!"));
                    writeBatch.Add("foo-bar", valueBuffer, "test-tree");

                    writeBatch.MultiAdd("multi-foo", "AA", "test-tree");
                    env.Writer.Write(writeBatch);
                }

                using (var writeBatch = new WriteBatch())
                {
                    writeBatch.Increment("incr-key", 5, "test-tree");
                    env.Writer.Write(writeBatch);
                }

                using (var tx = env.NewTransaction(TransactionFlags.Read))
                {
                    Assert.Equal(5, tx.ReadTree("test-tree").Read("incr-key").Reader.ReadLittleEndianInt64());

                    using (var writeBatch = new WriteBatch())
                    {
                        writeBatch.Increment("incr-key", 5, "test-tree");
                        env.Writer.Write(writeBatch);
                    }

                    Assert.Equal(5, tx.ReadTree("test-tree").Read("incr-key").Reader.ReadLittleEndianInt64());
                }

                using (var tx = env.NewTransaction(TransactionFlags.Read))
                {
                    Assert.Equal(10, tx.ReadTree("test-tree").Read("incr-key").Reader.ReadLittleEndianInt64());
                }

                using (var writeBatch = new WriteBatch())
                {
                    writeBatch.MultiAdd("multi-foo", "BB", "test-tree");
                    writeBatch.MultiAdd("multi-foo", "CC", "test-tree");

                    writeBatch.Delete("foo-bar", "test-tree");
                    env.Writer.Write(writeBatch);
                }

                using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
                {
                    env.CreateTree(tx, "test-tree2");
                    tx.Commit();
                }

                using (var writeBatch = new WriteBatch())
                {
                    var valueBuffer = new MemoryStream(Encoding.UTF8.GetBytes("testing testing 1!"));
                    writeBatch.Add("foo", valueBuffer, "test-tree2");

                    valueBuffer = new MemoryStream(Encoding.UTF8.GetBytes("testing testing 1 2!"));
                    writeBatch.Add("bar", valueBuffer, "test-tree2");

                    valueBuffer = new MemoryStream(Encoding.UTF8.GetBytes("testing testing 1 2 3!"));
                    writeBatch.Add("foo-bar", valueBuffer, "test-tree2");
                    env.Writer.Write(writeBatch);
                }

                using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
                {
                    env.CreateTree(tx, "structures-tree");
                    tx.Commit();
                }

                using (var writeBatch = new WriteBatch())
                {
                    writeBatch.AddStruct("structs/1", new Structure<SampleStruct>(structSchema)
                        .Set(SampleStruct.Foo, 13)
                        .Set(SampleStruct.Bar, "debug journal testing"),
                        "structures-tree");

                    env.Writer.Write(writeBatch);
                }

                using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
                {
                    env.CreateTree(tx, "rename-me");
                    tx.Commit();
                }

                using (var writeBatch = new WriteBatch())
                {
                    writeBatch.Add("item", "renaming tree test", "rename-me");

                    env.Writer.Write(writeBatch);
                }

                using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
                {
                    env.RenameTree(tx, "rename-me", "renamed");

                    tx.Commit();
                }
            }

            using (var env = new StorageEnvironment(StorageEnvironmentOptions.CreateMemoryOnly()))
            {
                env.DebugJournal = DebugJournal.FromFile(debugJouralName, env);
                env.DebugJournal.Replay();

                using (var snapshot = env.CreateSnapshot())
                {
                    Assert.Equal("{ \"title\": \"foo\",\"name\":\"bar\"}", snapshot.Read("test-tree", "foo").Reader.ToStringValue());
                    Assert.Equal("testing testing 1 2!", snapshot.Read("test-tree", "bar").Reader.ToStringValue());

                    Assert.Equal("testing testing 1!", snapshot.Read("test-tree2", "foo").Reader.ToStringValue());
                    Assert.Equal("testing testing 1 2!", snapshot.Read("test-tree2", "bar").Reader.ToStringValue());
                    Assert.Equal("testing testing 1 2 3!", snapshot.Read("test-tree2", "foo-bar").Reader.ToStringValue());

                    Assert.Equal(10, snapshot.Read("test-tree", "incr-key").Reader.ReadLittleEndianInt64());

                    Assert.Equal(0,snapshot.ReadVersion("test-tree","foo-bar"));

                    using (var iter = snapshot.MultiRead("test-tree","multi-foo"))
                    {
                        iter.Seek(Slice.BeforeAllKeys);
                        Assert.Equal("AA",iter.CurrentKey.ToString());
                        Assert.DoesNotThrow(() => iter.MoveNext());
                        Assert.Equal("BB",iter.CurrentKey.ToString());
                        Assert.DoesNotThrow(() => iter.MoveNext());
                        Assert.Equal("CC",iter.CurrentKey.ToString());
                    }

                    var structReader = snapshot.ReadStruct("structures-tree", "structs/1", structSchema).Reader;

                    Assert.Equal(13, structReader.ReadInt(SampleStruct.Foo));
                    Assert.Equal("debug journal testing", structReader.ReadString(SampleStruct.Bar));

                    Assert.Equal("renaming tree test", snapshot.Read("renamed", "item").Reader.ToStringValue());
                }
            }			

        }

        public enum SampleStruct
        {
            Foo,
            Bar
        }

        [PrefixesFact]
        public void Record_debug_journal_and_replay_it_size_only()
        {
            using (var env = new StorageEnvironment(StorageEnvironmentOptions.CreateMemoryOnly()))
            {
                env.DebugJournal = new DebugJournal(debugJouralName, env, true) { RecordOnlyValueLength = true };
                using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
                {
                    env.CreateTree(tx, "test-tree");
                    tx.Commit();
                }

                using (var writeBatch = new WriteBatch())
                {
                    var valueBuffer = new MemoryStream(Encoding.UTF8.GetBytes("{ \"title\": \"foo\",\"name\":\"bar\"}"));
                    writeBatch.Add("foo", valueBuffer, "test-tree");

                    valueBuffer = new MemoryStream(Encoding.UTF8.GetBytes("testing testing 1 2!"));
                    writeBatch.Add("bar", valueBuffer, "test-tree");

                    valueBuffer = new MemoryStream(Encoding.UTF8.GetBytes("testing testing 1 2 3!"));
                    writeBatch.Add("foo-bar", valueBuffer, "test-tree");

                    writeBatch.MultiAdd("multi-foo", "AA", "test-tree");
                    env.Writer.Write(writeBatch);
                }

                using (var writeBatch = new WriteBatch())
                {
                    writeBatch.Increment("incr-key", 5, "test-tree");
                    env.Writer.Write(writeBatch);
                }

                using (var tx = env.NewTransaction(TransactionFlags.Read))
                {
                    Assert.Equal(5, tx.ReadTree("test-tree").Read("incr-key").Reader.ReadLittleEndianInt64());

                    using (var writeBatch = new WriteBatch())
                    {
                        writeBatch.Increment("incr-key", 5, "test-tree");
                        env.Writer.Write(writeBatch);
                    }

                    Assert.Equal(5, tx.ReadTree("test-tree").Read("incr-key").Reader.ReadLittleEndianInt64());
                }

                using (var tx = env.NewTransaction(TransactionFlags.Read))
                {
                    Assert.Equal(10, tx.ReadTree("test-tree").Read("incr-key").Reader.ReadLittleEndianInt64());
                }

                using (var writeBatch = new WriteBatch())
                {
                    writeBatch.MultiAdd("multi-foo", "BB", "test-tree");
                    writeBatch.MultiAdd("multi-foo", "CC", "test-tree");

                    writeBatch.Delete("foo-bar", "test-tree");
                    env.Writer.Write(writeBatch);
                }

                using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
                {
                    env.CreateTree(tx, "test-tree2");
                    tx.Commit();
                }

                using (var writeBatch = new WriteBatch())
                {
                    var valueBuffer = new MemoryStream(Encoding.UTF8.GetBytes("testing testing 1!"));
                    writeBatch.Add("foo", valueBuffer, "test-tree2");

                    valueBuffer = new MemoryStream(Encoding.UTF8.GetBytes("testing testing 1 2!"));
                    writeBatch.Add("bar", valueBuffer, "test-tree2");

                    valueBuffer = new MemoryStream(Encoding.UTF8.GetBytes("testing testing 1 2 3!"));
                    writeBatch.Add("foo-bar", valueBuffer, "test-tree2");
                    env.Writer.Write(writeBatch);
                }
            }

            using (var env = new StorageEnvironment(StorageEnvironmentOptions.CreateMemoryOnly()))
            {
                env.DebugJournal = DebugJournal.FromFile(debugJouralName, env, true);
                env.DebugJournal.Replay();

                using (var snapshot = env.CreateSnapshot())
                {
                    Assert.Equal("{ \"title\": \"foo\",\"name\":\"bar\"}".Length, snapshot.Read("test-tree", "foo").Reader.Length);
                    Assert.Equal("testing testing 1 2!".Length, snapshot.Read("test-tree", "bar").Reader.Length);

                    Assert.Equal("testing testing 1!".Length, snapshot.Read("test-tree2", "foo").Reader.Length);
                    Assert.Equal("testing testing 1 2!".Length, snapshot.Read("test-tree2", "bar").Reader.Length);
                    Assert.Equal("testing testing 1 2 3!".Length, snapshot.Read("test-tree2", "foo-bar").Reader.Length);

                    Assert.Equal(10, snapshot.Read("test-tree", "incr-key").Reader.ReadLittleEndianInt64());

                    Assert.Equal(0, snapshot.ReadVersion("test-tree", "foo-bar"));

                    using (var iter = snapshot.MultiRead("test-tree", "multi-foo"))
                    {
                        iter.Seek(Slice.BeforeAllKeys);
                        Assert.Equal("AA", iter.CurrentKey.ToString());
                        Assert.DoesNotThrow(() => iter.MoveNext());
                        Assert.Equal("BB", iter.CurrentKey.ToString());
                        Assert.DoesNotThrow(() => iter.MoveNext());
                        Assert.Equal("CC", iter.CurrentKey.ToString());
                    }
                }
            }

        }

        [PrefixesFact]
        public void Record_debug_journal_and_replay_it_with_manual_flushing()
        {
            using (var env = new StorageEnvironment(StorageEnvironmentOptions.CreateMemoryOnly()))
            {
                env.DebugJournal = new DebugJournal(debugJouralName, env, true);
                using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
                {
                    env.CreateTree(tx, "test-tree");
                    tx.Commit();
                }

                using (var writeBatch = new WriteBatch())
                {
                    var valueBuffer = new MemoryStream(Encoding.UTF8.GetBytes("{ \"title\": \"foo\",\"name\":\"bar\"}"));
                    writeBatch.Add("foo", valueBuffer, "test-tree");
                    env.Writer.Write(writeBatch);
                }

                using (env.Options.AllowManualFlushing())
                {
                    env.FlushLogToDataFile();
                }

                using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
                using (env.Options.AllowManualFlushing())
                {
                    env.FlushLogToDataFile(tx);
                    tx.Commit();
                }
            }

            using (var env = new StorageEnvironment(StorageEnvironmentOptions.CreateMemoryOnly()))
            {
                env.DebugJournal = DebugJournal.FromFile(debugJouralName, env);
                env.DebugJournal.Replay();

                using (var snapshot = env.CreateSnapshot())
                {
                    Assert.Equal("{ \"title\": \"foo\",\"name\":\"bar\"}", snapshot.Read("test-tree", "foo").Reader.ToStringValue());
                }
            }

        }
    }
}
