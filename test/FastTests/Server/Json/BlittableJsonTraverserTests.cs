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

            object read;
            sut.TryRead(doc, "Name", out read);

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

            object read;
            sut.TryRead(doc, "Address.City", out read);

            Assert.Equal("New York City", read.ToString());

            sut.TryRead(doc, "Address.Details.Floor", out read);

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

            object read;
            sut.TryRead(doc, "Friends,Name", out read);

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

            object read;
            sut.TryRead(doc, "Friends,Name.First", out read);

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