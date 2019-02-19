using System.Text;
using Sparrow.Server;
using Voron;
using Xunit;

namespace FastTests.Voron.Tables
{
    public unsafe class CompositeIndex : TableStorageTest
    {

        [Fact]
        public void CanInsertThenReadByComposite()
        {
            using (var tx = Env.WriteTransaction())
            {
                DocsSchema.Create(tx, "docs", 16);

                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                var docs = tx.OpenTable(DocsSchema, "docs");
                SetHelper(docs, "users/1", "Users", 1L, "{'Name': 'Oren'}");
                SetHelper(docs, "users/2", "Users", 2L, "{'Name': 'Eini'}");

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var docs = tx.OpenTable(DocsSchema, "docs");
                Slice str;
                using (Slice.From(tx.Allocator, "Users", ByteStringType.Immutable, out str))
                {
                    var seekResults = docs.SeekForwardFrom(DocsSchema.Indexes[EtagAndCollectionSlice], str, 0).GetEnumerator();
                    Assert.True(seekResults.MoveNext());
                    var reader = seekResults.Current;

                    var valueReader = reader.Key.CreateReader();
                    Assert.Equal("Users", valueReader.ReadString(5));
                    Assert.Equal(1L, valueReader.ReadBigEndianInt64());
                    var handle = reader.Result.Reader;
                    int size;
                    Assert.Equal("{'Name': 'Oren'}", Encoding.UTF8.GetString(handle.Read(3, out size), size));

                    Assert.True(seekResults.MoveNext());
                    reader = seekResults.Current;

                    valueReader = reader.Key.CreateReader();
                    Assert.Equal("Users", valueReader.ReadString(5));
                    Assert.Equal(2L, valueReader.ReadBigEndianInt64());
                    handle = reader.Result.Reader;
                    Assert.Equal("{'Name': 'Eini'}", Encoding.UTF8.GetString(handle.Read(3, out size), size));

                    Assert.False(seekResults.MoveNext());
                }
                    
                tx.Commit();
            }
        }

        [Fact]
        public void CanInsertThenDeleteByComposite()
        {
            using (var tx = Env.WriteTransaction())
            {
                DocsSchema.Create(tx, "docs", 16);

                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                var docs = tx.OpenTable(DocsSchema, "docs");
                SetHelper(docs, "users/1", "Users", 1L, "{'Name': 'Oren'}");


                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                var docs = tx.OpenTable(DocsSchema, "docs");

                Slice key;
                Slice.From(tx.Allocator, "users/1", out key);
                docs.DeleteByKey(key);

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var docs = tx.OpenTable(DocsSchema, "docs");

                Slice str;
                using (Slice.From(tx.Allocator, "Users", ByteStringType.Immutable, out str))
                {
                    var reader = docs.SeekForwardFrom(DocsSchema.Indexes[EtagAndCollectionSlice], str, 0);
                    Assert.Empty(reader);
                }
            }
        }


        [Fact]
        public void CanInsertThenUpdateThenByComposite()
        {
            using (var tx = Env.WriteTransaction())
            {
                DocsSchema.Create(tx, "docs", 16);

                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                var docs = tx.OpenTable(DocsSchema, "docs");
                SetHelper(docs, "users/1", "Users", 1L, "{'Name': 'Oren'}");

                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                var docs = tx.OpenTable(DocsSchema, "docs");
                SetHelper(docs, "users/1", "Users", 2L, "{'Name': 'Eini'}");

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var docs = tx.OpenTable(DocsSchema, "docs");

                bool gotValues = false;
                Slice str;
                using (Slice.From(tx.Allocator, "Users", ByteStringType.Immutable, out str))
                {
                    foreach (var reader in docs.SeekForwardFrom(DocsSchema.Indexes[EtagAndCollectionSlice], str, 0))
                    {
                        var valueReader = reader.Key.CreateReader();
                        Assert.Equal("Users", valueReader.ReadString(5));
                        Assert.Equal(2L, valueReader.ReadBigEndianInt64());

                        var handle = reader.Result;
                        int size;
                        Assert.Equal("{'Name': 'Eini'}", Encoding.UTF8.GetString(handle.Reader.Read(3, out size), size));

                        tx.Commit();
                        gotValues = true;
                        break;
                    }
                }
                Assert.True(gotValues);
            }
        }

    }
}