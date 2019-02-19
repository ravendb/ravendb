using System;
using System.Collections.Generic;
using System.Text;
using FastTests;
using Voron;
using Xunit;

namespace SlowTests.Voron
{
    public class MultiAdds : NoDisposalNeeded
    {
        readonly Random _random = new Random(1234);

        private string RandomString(int size)
        {
            var builder = new StringBuilder();
            for (int i = 0; i < size; i++)
            {
                builder.Append(Convert.ToChar(Convert.ToInt32(Math.Floor(26 * _random.NextDouble() + 65))));
            }

            return builder.ToString();
        }

        [Theory]
        [InlineData(0500)]
        [InlineData(1000)]
        [InlineData(2000)]
        [InlineData(3000)]
        [InlineData(4000)]
        [InlineData(5000)]
        public void MultiAdds_And_MultiDeletes_After_Causing_PageSplit_DoNot_Fail(int size)
        {
            using (var Env = new StorageEnvironment(StorageEnvironmentOptions.CreateMemoryOnly()))
            {
                var inputData = new List<byte[]>();
                for (int i = 0; i < size; i++)
                {
                    inputData.Add(Encoding.UTF8.GetBytes(RandomString(1024)));
                }

                using (var tx = Env.WriteTransaction())
                {
                    tx.CreateTree( "foo");
                    tx.Commit();
                }

                using (var tx = Env.WriteTransaction())
                {
                    var tree = tx.CreateTree("foo");
                    foreach (var buffer in inputData)
                    {
                        Slice key;
                        Slice.From(tx.Allocator, buffer, out key);
                        tree.MultiAdd("ChildTreeKey", key);
                    }
                    tx.Commit();
                }
                
                using (var tx = Env.WriteTransaction())
                {
                    var tree = tx.CreateTree("foo");
                    for (int i = 0; i < inputData.Count; i++)
                    {
                        var buffer = inputData[i];
                        Slice key;
                        Slice.From(tx.Allocator, buffer, out key);
                        tree.MultiDelete("ChildTreeKey", key);
                    }

                    tx.Commit();
                }
            }
        }
    }
}
