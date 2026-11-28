using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using FastTests;
using Raven.Server.Documents;
using Raven.Server.Json;
using Sparrow.Json;
using Sparrow.Json.Sync;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Blittable
{
    public class DocumentFlagsWriting(ITestOutputHelper output) : NoDisposalNeeded(output)
    {
        [RavenFact(RavenTestCategory.Core)]
        public void Writes_every_combination_of_flags_like_ToString()
        {
            var named = Enum.GetValues<DocumentFlags>().Aggregate(0, (all, flag) => all | (int)flag);

            // every combination of the named bits, then values with a bit that has no name
            var values = new List<int>();
            for (int value = named; ; value = (value - 1) & named)
            {
                values.Add(value);
                if (value == 0)
                    break;
            }

            values.AddRange([1 << 18, (1 << 18) | (int)DocumentFlags.HasRevisions, 1 << 30, int.MinValue, -1, int.MaxValue]);

            using (var context = JsonOperationContext.ShortTermSingleUse())
            using (var stream = new MemoryStream())
            using (var writer = new BlittableJsonTextWriter(context, stream))
            {
                foreach (var value in values)
                {
                    var flags = (DocumentFlags)value;
                    writer.WriteDocumentFlags(flags);
                    writer.Flush();

                    var actual = Encoding.UTF8.GetString(stream.GetBuffer(), 0, (int)stream.Length);
                    Assert.True($"\"{flags}\"" == actual, $"0x{value:X}: expected \"{flags}\", got {actual}");

                    stream.SetLength(0);
                }
            }
        }
    }
}
