using System.Collections.Generic;

using Voron;
using Voron.Data.Tables;
using Voron.Util.Conversion;
using Xunit;

namespace FastTests.Voron.Tables
{
    public class Bugs : TableStorageTest
    {
        [Fact]
        public void CanInsertThenDeleteBySecondary2()
        {
            using (var tx = Env.WriteTransaction())
            {
                DocsSchema.Create(tx, "docs");

                tx.Commit();
            }

            for (int j = 0; j < 10; j++)
            {
                using (var tx = Env.WriteTransaction())
                {
                    var docs = new Table(DocsSchema, "docs", tx);

                    for (int i = 0; i < 1000; i++)
                    {
                        SetHelper(docs, "users/" + i, "Users", 1L + i, "{'Name': 'Oren'}");
                    }

                    tx.Commit();
                }

                using (var tx = Env.WriteTransaction())
                {
                    var docs = new Table(DocsSchema, "docs", tx);

                    var ids = new List<long>();
                    foreach (var sr in docs.SeekForwardFrom(DocsSchema.Indexes["Etags"], Slice.BeforeAllKeys))
                    {
                        foreach (var tvr in sr.Results)
                            ids.Add(tvr.Id);
                    }

                    foreach (var id in ids)
                        docs.Delete(id);

                    tx.Commit();
                }

                using (var tx = Env.ReadTransaction())
                {
                    var docs = new Table(DocsSchema, "docs", tx);

                    var reader = docs.SeekForwardFrom(DocsSchema.Indexes["Etags"], new Slice(EndianBitConverter.Big.GetBytes(1)));
                    Assert.Empty(reader);
                }
            }
        }
    }
}