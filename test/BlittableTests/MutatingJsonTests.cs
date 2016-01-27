// -----------------------------------------------------------------------
//  <copyright file="MutatingJsonTests.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.IO;
using System.Text;
using Raven.Abstractions.Linq;
using Raven.Server.Json;
using Raven.Server.Json.Parsing;
using Xunit;

namespace BlittableTests
{
    public unsafe class MutatingJsonTests
    {
        private const string InitialJson = @"{""Name"":""Oren"",""Dogs"":[""Arava"",""Oscar"",""Sunny""],""State"":{""Sleep"":false}}";

        [Fact]
        public void CanAddProperty()
        {
            AssertEqualAfterRoundTrip(source =>
            {
                source.Modifications = new DynamicJsonValue
                {
                    ["Age"] = 34
                };
            }, @"{""Name"":""Oren"",""Dogs"":[""Arava"",""Oscar"",""Sunny""],""State"":{""Sleep"":false},""Age"":34}");
        }

        [Fact]
        public void CanModifyArrayProperty()
        {
            AssertEqualAfterRoundTrip(source =>
            {
                object result;
                source.TryGetMember("Dogs", out result);
                var array = (BlittableJsonReaderArray)result;
                array.Modifications = new DynamicJsonArray
                {
                    "Phoebe"
                };
                array.Modifications.RemoveAt(2);
            }, @"{""Name"":""Oren"",""Dogs"":[""Arava"",""Oscar"",""Phoebe""],""State"":{""Sleep"":false}}");
        }


        [Fact]
        public void CanModifyNestedObjectProperty()
        {
            AssertEqualAfterRoundTrip(source =>
            {
                object result;
                source.TryGetMember("State", out result);
                var array = (BlittableJsonReaderObject)result;
                array.Modifications = new DynamicJsonValue
                {
                    ["Sleep"] = true
                };
            }, @"{""Name"":""Oren"",""Dogs"":[""Arava"",""Oscar"",""Sunny""],""State"":{""Sleep"":true}}");
        }

        [Fact]
        public void CanRemoveAndAddProperty()
        {
            AssertEqualAfterRoundTrip(source =>
            {
                source.Modifications = new DynamicJsonValue(source)
                {
                    ["Pie"] = 3.147
                };
                source.Modifications.Remove("Dogs");
            }, @"{""Name"":""Oren"",""State"":{""Sleep"":false},""Pie"":3.147}");
        }

        [Fact]
        public void CanAddAndRemoveProperty()
        {
            AssertEqualAfterRoundTrip(source =>
            {
                source.Modifications = new DynamicJsonValue(source)
                {

                };
                source.Modifications.Remove("Dogs");
            }, @"{""Name"":""Oren"",""State"":{""Sleep"":false}}");
        }

        private static void AssertEqualAfterRoundTrip(Action<BlittableJsonReaderObject> mutate, string expected)
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

                    mutate(readerObject);
                    var document = ctx.ReadObject(readerObject, "foo");
                    newDocMem = pool.Allocate(document.SizeInBytes);
                    document.CopyTo((byte*)newDocMem.Address);
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