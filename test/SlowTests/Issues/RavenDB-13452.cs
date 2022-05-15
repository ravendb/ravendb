using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using FastTests.Server.JavaScript;
using Sparrow.Json;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_13452 : RavenTestBase
    {
        public RavenDB_13452(ITestOutputHelper output) : base(output)
        {
        }

        private class Item
        {
            public Dictionary<string, string> Values { get; set; }

            public Item2 Item2 { get; set;  }
        }

        private class Item2
        {
            public Item3 Item3 { get; set; }
        }

        private class Item3
        {
            public List<Item4> List4 { get; set; }
        }

        private class Item4
        {
            public Dictionary<string, Item5> Dict5 { get; set; }
        }

        private class Item5
        {
            public Dictionary<string, string> Values { get; set; }
        }

        [Theory]
        [RavenData(JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public void CanModifyDictionaryWithPatch_Add(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Item
                    {
                        Values = new Dictionary<string, string>
                        {
                            { "Key1", "Value1" },
                            { "Key2", "Value2" }
                        }
                    }, "items/1");
                    session.SaveChanges();
                }
                using (var session = store.OpenSession())
                {
                    var item = session.Load<Item>("items/1");
                    session.Advanced.Patch(item, x => x.Values, dict => dict.Add(new KeyValuePair<string, string>("Key3", "Value3")));
                    session.SaveChanges();
                }
                using (var commands = store.Commands())
                {
                    var item = commands.Get("items/1").BlittableJson;
                    Assert.True(item.TryGet(nameof(Item.Values), out BlittableJsonReaderObject values));
                    Assert.Equal(3, values.Count);

                    Assert.True(values.TryGet("Key1", out string value));
                    Assert.Equal("Value1", value);
                    Assert.True(values.TryGet("Key2", out value));
                    Assert.Equal("Value2", value);
                    Assert.True(values.TryGet("Key3", out value));
                    Assert.Equal("Value3", value);
                }
            }
        }

        [Theory]
        [RavenData(JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public void CanModifyDictionaryWithPatch_Add2(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Item
                    {
                        Values = new Dictionary<string, string>
                        {
                            { "Key1", "Value1" },
                            { "Key2", "Value2" }
                        }
                    }, "items/1");
                    session.SaveChanges();
                }
                using (var session = store.OpenSession())
                {
                    var item = session.Load<Item>("items/1");
                    session.Advanced.Patch(item, x => x.Values, dict => dict.Add("Key3", "Value3"));
                    session.SaveChanges();
                }
                using (var commands = store.Commands())
                {
                    var item = commands.Get("items/1").BlittableJson;
                    Assert.True(item.TryGet(nameof(Item.Values), out BlittableJsonReaderObject values));
                    Assert.Equal(3, values.Count);

                    Assert.True(values.TryGet("Key1", out string value));
                    Assert.Equal("Value1", value);
                    Assert.True(values.TryGet("Key2", out value));
                    Assert.Equal("Value2", value);
                    Assert.True(values.TryGet("Key3", out value));
                    Assert.Equal("Value3", value);
                }
            }
        }

        [Theory]
        [RavenData(JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public void CanModifyDictionaryWithPatch_Add_WithVariables(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Item
                    {
                        Values = new Dictionary<string, string>
                        {
                            { "Key1", "Value1" },
                            { "Key2", "Value2" }
                        }
                    }, "items/1");
                    session.SaveChanges();
                }

                var item2 = new Item2
                {
                    Item3 = new Item3
                    {
                        List4 = new List<Item4>
                        {
                            new Item4(),
                            new Item4
                            {
                                Dict5 = new Dictionary<string, Item5>
                                {
                                    { "Key3", new Item5()},
                                    { "Key4", new Item5()},
                                    { "Key5", new Item5()}
                                }
                            },
                            new Item4()
                        }
                    }
                };

                var s = "aValue3z";

                using (var session = store.OpenSession())
                {
                    var item = session.Load<Item>("items/1");
                    session.Advanced.Patch(item, x => x.Values, dict => dict.Add(item2.Item3.List4[1].Dict5.Keys.First(), s.Substring(1, s.Length - 2)));
                    session.SaveChanges();
                }

                using (var commands = store.Commands())
                {
                    var item = commands.Get("items/1").BlittableJson;
                    Assert.True(item.TryGet(nameof(Item.Values), out BlittableJsonReaderObject values));
                    Assert.Equal(3, values.Count);
                    
                    Assert.True(values.TryGet("Key1", out string value));
                    Assert.Equal("Value1", value);
                    Assert.True(values.TryGet("Key2", out value));
                    Assert.Equal("Value2", value);
                    Assert.True(values.TryGet("Key3", out value));
                    Assert.Equal("Value3", value);
                }
            }
        }

        [Theory]
        [RavenData(JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public void CanModifyDictionaryWithPatch_Add_WithComplexNestedPath(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Item
                    {
                        Item2 = new Item2
                        {
                            Item3 = new Item3
                            {
                                List4 = new List<Item4>
                                {
                                    new Item4(),
                                    new Item4(),
                                    new Item4
                                    {
                                        Dict5 = new Dictionary<string, Item5>
                                        {
                                            {"foo", new Item5() },
                                            {"bar", new Item5() },
                                            {"Item5", 
                                                new Item5
                                                {
                                                    Values = new Dictionary<string, string>
                                                    {
                                                        { "Key1", "Value1" },
                                                        { "Key2", "Value2" }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }, "items/1");
                    session.SaveChanges();
                }
                using (var session = store.OpenSession())
                {
                    var item = session.Load<Item>("items/1");
                    session.Advanced.Patch(item, x => x.Item2.Item3.List4.Last().Dict5["Item5"].Values, dict => dict.Add("Key3", "Value3"));
                    session.SaveChanges();
                }
                using (var commands = store.Commands())
                {
                    var item = commands.Get("items/1").BlittableJson;
                    Assert.True(item.TryGet(nameof(Item2), out BlittableJsonReaderObject item2));
                    Assert.True(item2.TryGet(nameof(Item3), out BlittableJsonReaderObject item3));
                    Assert.True(item3.TryGet(nameof(Item3.List4), out BlittableJsonReaderArray list4));
                    var item4 = list4.Last() as BlittableJsonReaderObject;
                    Assert.NotNull(item4);
                    Assert.True(item4.TryGet(nameof(Item4.Dict5), out BlittableJsonReaderObject dict5));
                    Assert.True(dict5.TryGet(nameof(Item5), out BlittableJsonReaderObject item5));
                    Assert.True(item5.TryGet(nameof(Item5.Values), out BlittableJsonReaderObject values));
                    Assert.Equal(3, values.Count);

                    Assert.True(values.TryGet("Key1", out string value));
                    Assert.Equal("Value1", value);
                    Assert.True(values.TryGet("Key2", out value));
                    Assert.Equal("Value2", value);
                    Assert.True(values.TryGet("Key3", out value));
                    Assert.Equal("Value3", value);
                }
            }
        }

        [Theory]
        [RavenData(JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public async Task CanModifyDictionaryWithPatch_Add_AsyncSession(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Item
                    {
                        Values = new Dictionary<string, string>
                        {
                            { "Key1", "Value1" },
                            { "Key2", "Value2" }
                        }
                    }, "items/1");
                    await session.SaveChangesAsync();
                }
                using (var session = store.OpenAsyncSession())
                {
                    var item = await session.LoadAsync<Item>("items/1");
                    session.Advanced.Patch(item, x => x.Values, dict => dict.Add(new KeyValuePair<string, string>("Key3", "Value3")));
                    await session.SaveChangesAsync();
                }
                using (var commands = store.Commands())
                {
                    var item = commands.Get("items/1").BlittableJson;
                    Assert.True(item.TryGet(nameof(Item.Values), out BlittableJsonReaderObject values));
                    Assert.Equal(3, values.Count);

                    Assert.True(values.TryGet("Key1", out string value));
                    Assert.Equal("Value1", value);
                    Assert.True(values.TryGet("Key2", out value));
                    Assert.Equal("Value2", value);
                    Assert.True(values.TryGet("Key3", out value));
                    Assert.Equal("Value3", value);
                }
            }
        }

        [Theory]
        [RavenData(JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public async Task CanModifyDictionaryWithPatch_Add2_AsyncSession(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Item
                    {
                        Values = new Dictionary<string, string>
                        {
                            { "Key1", "Value1" },
                            { "Key2", "Value2" }
                        }
                    }, "items/1");
                    await session.SaveChangesAsync();
                }
                using (var session = store.OpenAsyncSession())
                {
                    var item = await session.LoadAsync<Item>("items/1");
                    session.Advanced.Patch(item, x => x.Values, dict => dict.Add("Key3", "Value3"));
                    await session.SaveChangesAsync();
                }
                using (var commands = store.Commands())
                {
                    var item = commands.Get("items/1").BlittableJson;
                    Assert.True(item.TryGet(nameof(Item.Values), out BlittableJsonReaderObject values));
                    Assert.Equal(3, values.Count);

                    Assert.True(values.TryGet("Key1", out string value));
                    Assert.Equal("Value1", value);
                    Assert.True(values.TryGet("Key2", out value));
                    Assert.Equal("Value2", value);
                    Assert.True(values.TryGet("Key3", out value));
                    Assert.Equal("Value3", value);
                }
            }
        }


        [Theory]
        [RavenData(JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public void CanModifyDictionaryWithPatch_Remove(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Item
                    {
                        Values = new Dictionary<string, string>
                        {
                            { "Key1", "Value1" },
                            { "Key2", "Value2" },
                            { "Key3", "Value3" }
                        }
                    }, "items/1");
                    session.SaveChanges();
                }
                using (var session = store.OpenSession())
                {
                    var item = session.Load<Item>("items/1");
                    session.Advanced.Patch(item, x => x.Values, dict => dict.Remove("Key2"));
                    session.SaveChanges();
                }
                using (var commands = store.Commands())
                {
                    var item = commands.Get("items/1").BlittableJson;
                    Assert.True(item.TryGet(nameof(Item.Values), out BlittableJsonReaderObject values));
                    Assert.Equal(2, values.Count);

                    Assert.True(values.TryGet("Key1", out string value));
                    Assert.Equal("Value1", value);
                    Assert.True(values.TryGet("Key3", out value));
                    Assert.Equal("Value3", value);
                }
            }
        }


        [Theory]
        [RavenData(JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public void CanModifyDictionaryWithPatch_Remove_WithVariable(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    var item = new Item
                    {
                        Values = new Dictionary<string, string>
                        {
                            { "Key1", "Value1" },
                            { "Key2", "Value2" },
                            { "Key3", "Value3" }
                        }
                    };
                    session.Store(item, "items/1");
                    session.SaveChanges();
                }
                using (var session = store.OpenSession())
                {
                    var item = session.Load<Item>("items/1");
                    var toRemove = item.Values.Keys.ToList()[1];

                    session.Advanced.Patch(item, x => x.Values, dict => dict.Remove(toRemove));
                    session.SaveChanges();
                }
                using (var commands = store.Commands())
                {
                    var item = commands.Get("items/1").BlittableJson;
                    Assert.True(item.TryGet(nameof(Item.Values), out BlittableJsonReaderObject values));
                    Assert.Equal(2, values.Count);

                    Assert.True(values.TryGet("Key1", out string value));
                    Assert.Equal("Value1", value);
                    Assert.True(values.TryGet("Key3", out value));
                    Assert.Equal("Value3", value);
                }
            }
        }

        [Theory]
        [RavenData(JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public async Task CanModifyDictionaryWithPatch_Remove_AsyncSession(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Item
                    {
                        Values = new Dictionary<string, string>
                        {
                            { "Key1", "Value1" },
                            { "Key2", "Value2" },
                            { "Key3", "Value3" }
                        }
                    }, "items/1");
                    await session.SaveChangesAsync();
                }
                using (var session = store.OpenAsyncSession())
                {
                    var item = await session.LoadAsync<Item>("items/1");
                    session.Advanced.Patch(item, x => x.Values, dict => dict.Remove("Key2"));
                    await session.SaveChangesAsync();
                }
                using (var commands = store.Commands())
                {
                    var item = commands.Get("items/1").BlittableJson;
                    Assert.True(item.TryGet(nameof(Item.Values), out BlittableJsonReaderObject values));
                    Assert.Equal(2, values.Count);

                    Assert.True(values.TryGet("Key1", out string value));
                    Assert.Equal("Value1", value);
                    Assert.True(values.TryGet("Key3", out value));
                    Assert.Equal("Value3", value);
                }
            }
        }
    }
}
