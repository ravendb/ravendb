using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Threading;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Blittable.BlittableJsonWriterTests
{
    // IMPORTANT: The following tests exemplify the behavior of the escaping rules as they were defined in 2015.
    // The escaping rules vary depending on the context of which character is valid for the usual protocols we
    // use to dump the strings directly to the network. 
    //
    // The escaping rules are defined in the following file:
    //    - If the value is one of the following characters, it will not be escaped but their position be recorded:
    //          8  => '\b' => 0000 1000
    //          9  => '\t' => 0000 1001
    //          13 => '\r' => 0000 1101
    //          10 => '\n' => 0000 1010
    //          12 => '\f' => 0000 1100
    //          34 => '"'  => 0010 0010
    //          92 => '\\' => 0101 1100
    //    - If the value is a control character (strictly smaller than 32 in the ASCII table and not an escape character),
    //      it will be written with the nomenclature \u00XX, where XX is the hexadecimal representation of the character.
    //      However, for the purposes of the recording mechanism, the blittable format will be oblivious to the 
    //      existence of those characters and do not perform the conversion back. The control character is effectively
    //      converted to a string representation of the hexadecimal value while writing and remain as is.
    //
    // More details on the rationale in the following blog posts:
    // https://ayende.com/blog/172961/fastest-code-is-the-one-not-run-part-i-the-cost-of-escaping-strings

    public class BlittableEscapingTests(ITestOutputHelper output) : NoDisposalNeeded(output)
    {
        [RavenFact(RavenTestCategory.Core)]
        public void SingleControlCharacter()
        {
            using (var context = new JsonOperationContext(1024, 1024 * 4, 32 * 1024, SharedMultipleUseFlag.None))
            {
                using (var builder = new ManualBlittableJsonDocumentBuilder<UnmanagedWriteBuffer>(context))
                {
                    builder.Reset(BlittableJsonDocumentBuilder.UsageMode.None);
                    builder.StartWriteObjectDocument();

                    builder.StartWriteObject();
                    {
                        builder.WritePropertyName("Single");
                        builder.WriteValue("\u0001");
                    }
                    builder.WriteObjectEnd();

                    builder.FinalizeDocument();

                    var reader = builder.CreateReader();

                    Assert.Equal(1, reader.Count);
                    Assert.Equal(@"\u0001", reader["Single"].ToString());
                }
            }
        }

        [RavenFact(RavenTestCategory.Core)]
        public void SingleEscapeCharacter()
        {
            using (var context = new JsonOperationContext(1024, 1024 * 4, 32 * 1024, SharedMultipleUseFlag.None))
            {
                using (var builder = new ManualBlittableJsonDocumentBuilder<UnmanagedWriteBuffer>(context))
                {
                    builder.Reset(BlittableJsonDocumentBuilder.UsageMode.None);
                    builder.StartWriteObjectDocument();

                    builder.StartWriteObject();
                    {
                        builder.WritePropertyName("Single");
                        builder.WriteValue("\t");
                    }
                    builder.WriteObjectEnd();

                    builder.FinalizeDocument();

                    var reader = builder.CreateReader();

                    Assert.Equal(1, reader.Count);
                    Assert.Equal("\t", reader["Single"].ToString());
                }
            }
        }

        [RavenFact(RavenTestCategory.Core)]
        public void SingleControlCharacterAsLast()
        {
            using (var context = new JsonOperationContext(1024, 1024 * 4, 32 * 1024, SharedMultipleUseFlag.None))
            {
                using (var builder = new ManualBlittableJsonDocumentBuilder<UnmanagedWriteBuffer>(context))
                {
                    builder.Reset(BlittableJsonDocumentBuilder.UsageMode.None);
                    builder.StartWriteObjectDocument();

                    builder.StartWriteObject();
                    {
                        builder.WritePropertyName("Single");
                        builder.WriteValue("Cool\u0001");
                    }
                    builder.WriteObjectEnd();

                    builder.FinalizeDocument();

                    var reader = builder.CreateReader();

                    Assert.Equal(1, reader.Count);
                    Assert.Equal(@"Cool\u0001", reader["Single"].ToString());
                }
            }
        }

        [RavenFact(RavenTestCategory.Core)]
        public void SingleEscapeCharacterAsLast()
        {
            using (var context = new JsonOperationContext(1024, 1024 * 4, 32 * 1024, SharedMultipleUseFlag.None))
            {
                using (var builder = new ManualBlittableJsonDocumentBuilder<UnmanagedWriteBuffer>(context))
                {
                    builder.Reset(BlittableJsonDocumentBuilder.UsageMode.None);
                    builder.StartWriteObjectDocument();

                    builder.StartWriteObject();
                    {
                        builder.WritePropertyName("Single");
                        builder.WriteValue("Cool\t");
                    }
                    builder.WriteObjectEnd();

                    builder.FinalizeDocument();

                    var reader = builder.CreateReader();

                    Assert.Equal(1, reader.Count);
                    Assert.Equal("Cool\t", reader["Single"].ToString());
                }
            }
        }

        [RavenFact(RavenTestCategory.Core)]
        public void SingleControlCharacterInTheMiddle()
        {
            using (var context = new JsonOperationContext(1024, 1024 * 4, 32 * 1024, SharedMultipleUseFlag.None))
            {
                using (var builder = new ManualBlittableJsonDocumentBuilder<UnmanagedWriteBuffer>(context))
                {
                    builder.Reset(BlittableJsonDocumentBuilder.UsageMode.None);
                    builder.StartWriteObjectDocument();

                    builder.StartWriteObject();
                    {
                        builder.WritePropertyName("Single");
                        builder.WriteValue("Cool\u0001Cool");
                    }
                    builder.WriteObjectEnd();

                    builder.FinalizeDocument();

                    var reader = builder.CreateReader();

                    Assert.Equal(1, reader.Count);
                    Assert.Equal(@"Cool\u0001Cool", reader["Single"].ToString());
                }
            }
        }

        [RavenFact(RavenTestCategory.Core)]
        public void SingleControlCharacterAsFirst()
        {
            using (var context = new JsonOperationContext(1024, 1024 * 4, 32 * 1024, SharedMultipleUseFlag.None))
            {
                using (var builder = new ManualBlittableJsonDocumentBuilder<UnmanagedWriteBuffer>(context))
                {
                    builder.Reset(BlittableJsonDocumentBuilder.UsageMode.None);
                    builder.StartWriteObjectDocument();

                    builder.StartWriteObject();
                    {
                        builder.WritePropertyName("Single");
                        builder.WriteValue("\u0002Cool");
                    }
                    builder.WriteObjectEnd();

                    builder.FinalizeDocument();

                    var reader = builder.CreateReader();

                    Assert.Equal(1, reader.Count);
                    Assert.Equal(@"\u0002Cool", reader["Single"].ToString());
                }
            }
        }

        [RavenFact(RavenTestCategory.Core)]
        public void MultipleControlCharacterWithIntermixedEscapeOnes()
        {
            using (var context = new JsonOperationContext(1024, 1024 * 4, 32 * 1024, SharedMultipleUseFlag.None))
            {
                using (var builder = new ManualBlittableJsonDocumentBuilder<UnmanagedWriteBuffer>(context))
                {
                    builder.Reset(BlittableJsonDocumentBuilder.UsageMode.None);
                    builder.StartWriteObjectDocument();

                    builder.StartWriteObject();
                    {
                        builder.WritePropertyName("Single");
                        builder.WriteValue("\u0002Cool\u0002Cool\t\u0002");
                    }
                    builder.WriteObjectEnd();

                    builder.FinalizeDocument();

                    var reader = builder.CreateReader();

                    Assert.Equal(1, reader.Count);
                    Assert.Equal("\\u0002Cool\\u0002Cool\t\\u0002", reader["Single"].ToString());
                }
            }
        }
    }
}
