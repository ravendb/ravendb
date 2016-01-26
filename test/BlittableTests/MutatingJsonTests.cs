// -----------------------------------------------------------------------
//  <copyright file="MutatingJsonTests.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.IO;
using System.Text;
using Raven.Server.Json;
using Raven.Server.Json.Parsing;
using Xunit;

namespace BlittableTests
{
    public unsafe class MutatingJsonTests
    {
        private const string InitialJson = @"{""Name"":""Oren"", ""Dogs"":[""Arava"",""Oscar"",""Sunny""]}";

        [Fact]
        public void CanAddProperty()
        {
            AssertEqualAfterRoundTrip(source =>
            {
                var modification = new DynamicJsonBuilder
                {
                    Value =
                    {
                        Source = source
                    },
                    ["Age"] = 34
                };
                return modification;
            }, @"{""Name"":""Oren"", ""Dogs"":[""Arava"",""Oscar"",""Sunny""],""Age"":34}");
        }

        private static void AssertEqualAfterRoundTrip(Func<BlittableJsonReaderObject, DynamicJsonBuilder>  mutate, string expected)
        {
            using (var pool = new UnmanagedBuffersPool("foo"))
            using (var ctx = new RavenOperationContext(pool))
            {
                var stream = new MemoryStream();
                var streamWriter = new StreamWriter(stream);
                streamWriter.Write(InitialJson);
                streamWriter.Flush();
                stream.Position = 0;
                var writer = ctx.Read(stream, "foo");
                UnmanagedBuffersPool.AllocatedMemoryData newDocMem = null;
                var allocatedMemoryData = pool.Allocate(writer.SizeInBytes);
                try
                {
                    var address = (byte*)allocatedMemoryData.Address;
                    writer.CopyTo(address);

                    var readerObject = new BlittableJsonReaderObject(address, writer.SizeInBytes, ctx);

                    var builder = mutate(readerObject);

                    var document = ctx.ReadObject(builder, "foo");
                    newDocMem = pool.Allocate(document.SizeInBytes);

                    readerObject = new BlittableJsonReaderObject((byte*)newDocMem.Address, document.SizeInBytes, ctx);
                    var ms = new MemoryStream();
                    readerObject.WriteTo(ms, originalPropertyOrder: true);
                    var actual = Encoding.UTF8.GetString(ms.ToArray());
                    Assert.Equal(expected, actual);
                }
                finally
                {
                    pool.Return(allocatedMemoryData);
                    if (newDocMem != null)
                        pool.Return(newDocMem);
                }
            }
        }
    }
}