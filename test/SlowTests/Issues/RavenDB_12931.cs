using FastTests.Voron.Tables;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_12931 : TableStorageTest
    {
        [Fact]
        public void CanDeleteTable()
        {
            using (var tx = Env.WriteTransaction())
            {
                DocsSchema.Create(tx, "docs", 16);

                tx.Commit();
            }

            for (int j = 0; j < 10; j++)
            {
                using (var tx = Env.WriteTransaction())
                {
                    var docs = tx.OpenTable(DocsSchema, "docs");

                    for (int i = 0; i < 1000; i++)
                    {
                        SetHelper(docs, "users/" + i, "Users", 1L + i, "{'Name': 'Oren'}");
                    }

                    tx.Commit();
                }
            }

            using (var tx = Env.WriteTransaction())
            {
                tx.DeleteTable("docs");

                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                Assert.Null(tx.LowLevelTransaction.RootObjects.Read("docs"));
            }
        }
    }
}
