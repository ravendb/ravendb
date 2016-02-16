using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Server.Json;
using Raven.Server.Json.Parsing;
using Raven.Server.ServerWide.Context;

using Xunit;

namespace FastTests.Server.Json
{
    public class BlittableJsonTraverserTests : IDisposable
    {
        private readonly UnmanagedBuffersPool _pool;
        private readonly MemoryOperationContext _ctx;
        private readonly List<BlittableJsonReaderObject> _docs = new List<BlittableJsonReaderObject>();

        public BlittableJsonTraverserTests()
        {
            _pool = new UnmanagedBuffersPool("foo");
            _ctx = new MemoryOperationContext(_pool);
        }

        [Fact]
        public void Reads_simple_value()
        {
            var sut = new BlittableJsonTraverser();

            var doc = create_doc(new DynamicJsonValue
            {
                ["Name"] = "John Doe"
            });

            var read = sut.Read(doc, "Name");

            Assert.Equal("John Doe", read.ToString());
        }

        [Fact]
        public void Reads_nested_values()
        {
            var sut = new BlittableJsonTraverser();

            var doc = create_doc(new DynamicJsonValue
            {
                ["Name"] = "John Doe",
                ["Address"] = new DynamicJsonValue
                {
                    ["City"] = "New York City",
                    ["Details"] = new DynamicJsonValue
                    {
                        ["Floor"] = 2
                    }
                }
            });

            var read = sut.Read(doc, "Address.City");

            Assert.Equal("New York City", read.ToString());

            read = sut.Read(doc, "Address.Details.Floor");

            Assert.Equal(2L, read);
        }

        [Fact]
        public void Reads_values_nested_in_array()
        {
            var sut = new BlittableJsonTraverser();

            var doc = create_doc(new DynamicJsonValue
            {
                ["Friends"] = new DynamicJsonArray
                {
                    Items = new Queue<object>(new[]
                    {
                        new DynamicJsonValue
                        {
                            ["Name"] = "Joe"
                        },
                        new DynamicJsonValue
                        {
                            ["Name"] = "John"
                        }
                    })
                }
            });

            var read = sut.Read(doc, "Friends,Name");

            var enumerable = read as IEnumerable<object>;

            Assert.NotNull(enumerable);

            var items = enumerable.ToList();

            Assert.Equal(2, items.Count);
            Assert.Equal("Joe", items[0].ToString());
            Assert.Equal("John", items[1].ToString());
        }

        [Fact]
        public void Reads_values_nested_in_array_as_objects()
        {
            var sut = new BlittableJsonTraverser();

            var doc = create_doc(new DynamicJsonValue
            {
                ["Friends"] = new DynamicJsonArray
                {
                    Items = new Queue<object>(new[]
                    {
                        new DynamicJsonValue
                        {
                            ["Name"] = new DynamicJsonValue
                            {
                                ["First"] = "Joe"
                            }
                        },
                        new DynamicJsonValue
                        {
                            ["Name"] = new DynamicJsonValue
                            {
                                ["First"] = "John"
                            }
                        },
                    })
                }
            });

            var read = sut.Read(doc, "Friends,Name.First");

            var enumerable = read as IEnumerable<object>;

            Assert.NotNull(enumerable);

            var items = enumerable.ToList();

            Assert.Equal(2, items.Count);
            Assert.Equal("Joe", items[0].ToString());
            Assert.Equal("John", items[1].ToString());
        }

        public BlittableJsonReaderObject create_doc(DynamicJsonValue document)
        {
            var doc = _ctx.ReadObject(document, string.Empty);

            _docs.Add(doc);

            return doc;
        }

        public void Dispose()
        {
            foreach (var docReader in _docs)
            {
                docReader.Dispose();
            }

            _ctx.Dispose();
            _pool.Dispose();
        }
    }
}