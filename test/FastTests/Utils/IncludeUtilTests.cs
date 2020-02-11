using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Utils
{
    public class IncludeUtilTests : NoDisposalNeeded
    {
        public IncludeUtilTests(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void FindDocIdFromPath_with_array_of_nested_objects_should_work1()
        {
            var obj = new DynamicJsonValue
            {
                ["Name"] = "John Doe",
                ["Foo"] = new DynamicJsonValue
                {
                    ["Bar"] = new DynamicJsonValue
                    {
                        ["C"] = new DynamicJsonArray(new[]
                        {
                            new DynamicJsonValue
                            {
                                ["X"] = new DynamicJsonValue
                                {
                                    ["Y"] = "ccc/1"
                                }
                            },
                            new DynamicJsonValue
                            {
                                ["X"] = new DynamicJsonValue
                                {
                                    ["Y"] = "ccc/2"
                                }
                            },
                            new DynamicJsonValue
                            {
                                ["X"] = new DynamicJsonValue
                                {
                                    ["Y"] = "ccc/3"
                                }
                            },
                            new DynamicJsonValue
                            {
                                ["X"] = new DynamicJsonValue
                                {
                                    ["YD"] = "ccc/4"
                                }
                            },
                            new DynamicJsonValue
                            {
                                ["X"] = new DynamicJsonValue
                                {
                                    ["Y"] = "ccc/5"
                                }
                            },
                        })
                    }
                },
                ["ContactInfoId"] = new DynamicJsonArray(new[]
                {
                    new DynamicJsonValue
                    {
                        ["Foo"] = "foobar/1"
                    },
                    new DynamicJsonValue
                    {
                        ["Foo"] = "foobar/2"
                    },
                    new DynamicJsonValue
                    {
                        ["Foo"] = "foobar/3"
                    },
                    new DynamicJsonValue
                    {
                        ["Foo"] = "foobar/4"
                    },
                })
            };

            using (var context = JsonOperationContext.ShortTermSingleUse())
            using (var reader = context.ReadObject(obj, "foo"))
            {
                var ids = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                IncludeUtil.GetDocIdFromInclude(reader, "ContactInfoId[].Foo", ids, '/');
                Assert.Equal(new[] { "foobar/1", "foobar/2", "foobar/3", "foobar/4" }, ids);

                ids.Clear();
                IncludeUtil.GetDocIdFromInclude(reader, "Foo.Bar.C[].X.Y", ids, '/');
                Assert.Equal(new[] { "ccc/1", "ccc/2", "ccc/3", "ccc/5" }, ids);
            }
        }

        [Theory]
        [InlineData('/')]
        [InlineData(':')]
        public void FindDocIdFromPath_with_array_of_arrays_with_simple_values_should_work(char identityPartsSeparator)
        {
            var obj = new DynamicJsonValue
            {
                ["Name"] = "John Doe",
                ["ContactInfoId"] = new DynamicJsonArray(new[]
                {
                    GetStringArray("foo", identityPartsSeparator),
                    GetStringArray("bar", identityPartsSeparator),
                    GetStringArray("foobar", identityPartsSeparator)
                })
            };

            using (var context = JsonOperationContext.ShortTermSingleUse())
            using (var reader = context.ReadObject(obj, "foo"))
            {
                var ids = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                IncludeUtil.GetDocIdFromInclude(reader, "ContactInfoId[].[].", ids, identityPartsSeparator);
                Assert.Equal(new[] { $"foo{identityPartsSeparator}1", $"foo{identityPartsSeparator}2", $"foo{identityPartsSeparator}3", $"bar{identityPartsSeparator}1", $"bar{identityPartsSeparator}2", $"bar{identityPartsSeparator}3", $"foobar{identityPartsSeparator}1", $"foobar{identityPartsSeparator}2", $"foobar{identityPartsSeparator}3" }, ids);

                ids.Clear();
                IncludeUtil.GetDocIdFromInclude(reader, $"ContactInfoId[].[].(abc{identityPartsSeparator})", ids, identityPartsSeparator);
                Assert.Equal(new[] { $"abc{identityPartsSeparator}foo{identityPartsSeparator}1", $"foo{identityPartsSeparator}1",
                    $"abc{identityPartsSeparator}foo{identityPartsSeparator}2", $"foo{identityPartsSeparator}2",
                    $"abc{identityPartsSeparator}foo{identityPartsSeparator}3", $"foo{identityPartsSeparator}3",
                    $"abc{identityPartsSeparator}bar{identityPartsSeparator}1", $"bar{identityPartsSeparator}1",
                    $"abc{identityPartsSeparator}bar{identityPartsSeparator}2", $"bar{identityPartsSeparator}2",
                    $"abc{identityPartsSeparator}bar{identityPartsSeparator}3", $"bar{identityPartsSeparator}3",
                    $"abc{identityPartsSeparator}foobar{identityPartsSeparator}1", $"foobar{identityPartsSeparator}1",
                    $"abc{identityPartsSeparator}foobar{identityPartsSeparator}2", $"foobar{identityPartsSeparator}2",
                    $"abc{identityPartsSeparator}foobar{identityPartsSeparator}3", $"foobar{identityPartsSeparator}3" }, ids);

                ids.Clear();
                IncludeUtil.GetDocIdFromInclude(reader, $"ContactInfoId[].[].[{{0}}{identityPartsSeparator}abc]", ids, identityPartsSeparator);
                Assert.Equal(new[] { $"foo{identityPartsSeparator}1{identityPartsSeparator}abc", $"foo{identityPartsSeparator}1",
                    $"foo{identityPartsSeparator}2{identityPartsSeparator}abc", $"foo{identityPartsSeparator}2",
                    $"foo{identityPartsSeparator}3{identityPartsSeparator}abc", $"foo{identityPartsSeparator}3",
                    $"bar{identityPartsSeparator}1{identityPartsSeparator}abc", $"bar{identityPartsSeparator}1",
                    $"bar{identityPartsSeparator}2{identityPartsSeparator}abc", $"bar{identityPartsSeparator}2",
                    $"bar{identityPartsSeparator}3{identityPartsSeparator}abc", $"bar{identityPartsSeparator}3",
                    $"foobar{identityPartsSeparator}1{identityPartsSeparator}abc", $"foobar{identityPartsSeparator}1",
                    $"foobar{identityPartsSeparator}2{identityPartsSeparator}abc", $"foobar{identityPartsSeparator}2",
                    $"foobar{identityPartsSeparator}3{identityPartsSeparator}abc", $"foobar{identityPartsSeparator}3" }, ids);
            }
        }

        [Theory]
        [InlineData('/')]
        [InlineData(':')]
        public void FindDocIdFromPath_with_array_of_arrays_of_objects_should_work(char identityPartsSeparator)
        {
            var obj = new DynamicJsonValue
            {
                ["Name"] = "John Doe",
                ["ContactInfoId"] = new DynamicJsonArray(new[]
                {
                    GetObjectArray("foo", identityPartsSeparator),
                    GetObjectArray("bar", identityPartsSeparator),
                    GetObjectArray("foobar", identityPartsSeparator)
                })
            };

            using (var context = JsonOperationContext.ShortTermSingleUse())
            using (var reader = context.ReadObject(obj, "foo"))
            {
                var ids = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                IncludeUtil.GetDocIdFromInclude(reader, "ContactInfoId[].[].Foo", ids, identityPartsSeparator);
                Assert.Equal(new[] { $"foo{identityPartsSeparator}1", $"foo{identityPartsSeparator}2", $"foo{identityPartsSeparator}3", $"bar{identityPartsSeparator}1", $"bar{identityPartsSeparator}2", $"bar{identityPartsSeparator}3", $"foobar{identityPartsSeparator}1", $"foobar{identityPartsSeparator}2", $"foobar{identityPartsSeparator}3" }, ids);

                ids.Clear();
                IncludeUtil.GetDocIdFromInclude(reader, $"ContactInfoId[].[].Foo(abc{identityPartsSeparator})", ids, identityPartsSeparator);
                Assert.Equal(new[] { $"abc{identityPartsSeparator}foo{identityPartsSeparator}1", $"foo{identityPartsSeparator}1",
                    $"abc{identityPartsSeparator}foo{identityPartsSeparator}2", $"foo{identityPartsSeparator}2",
                    $"abc{identityPartsSeparator}foo{identityPartsSeparator}3", $"foo{identityPartsSeparator}3",
                    $"abc{identityPartsSeparator}bar{identityPartsSeparator}1", $"bar{identityPartsSeparator}1",
                    $"abc{identityPartsSeparator}bar{identityPartsSeparator}2", $"bar{identityPartsSeparator}2",
                    $"abc{identityPartsSeparator}bar{identityPartsSeparator}3", $"bar{identityPartsSeparator}3",
                    $"abc{identityPartsSeparator}foobar{identityPartsSeparator}1", $"foobar{identityPartsSeparator}1",
                    $"abc{identityPartsSeparator}foobar{identityPartsSeparator}2", $"foobar{identityPartsSeparator}2",
                    $"abc{identityPartsSeparator}foobar{identityPartsSeparator}3", $"foobar{identityPartsSeparator}3" }, ids);


                ids.Clear();
                IncludeUtil.GetDocIdFromInclude(reader, $"ContactInfoId[].[].Foo[{{0}}{identityPartsSeparator}abc]", ids, identityPartsSeparator);
                Assert.Equal(new[] { $"foo{identityPartsSeparator}1{identityPartsSeparator}abc", $"foo{identityPartsSeparator}1",
                    $"foo{identityPartsSeparator}2{identityPartsSeparator}abc", $"foo{identityPartsSeparator}2",
                    $"foo{identityPartsSeparator}3{identityPartsSeparator}abc", $"foo{identityPartsSeparator}3",
                    $"bar{identityPartsSeparator}1{identityPartsSeparator}abc", $"bar{identityPartsSeparator}1",
                    $"bar{identityPartsSeparator}2{identityPartsSeparator}abc", $"bar{identityPartsSeparator}2",
                    $"bar{identityPartsSeparator}3{identityPartsSeparator}abc", $"bar{identityPartsSeparator}3",
                    $"foobar{identityPartsSeparator}1{identityPartsSeparator}abc", $"foobar{identityPartsSeparator}1",
                    $"foobar{identityPartsSeparator}2{identityPartsSeparator}abc", $"foobar{identityPartsSeparator}2",
                    $"foobar{identityPartsSeparator}3{identityPartsSeparator}abc", $"foobar{identityPartsSeparator}3" }, ids);
            }
        }

        [Theory]
        [InlineData('/')]
        [InlineData(':')]
        public void FindDocIdFromPath_with_array_of_with_of_arrays_of_nested_objects_should_work1(char identityPartsSeparator)
        {
            var obj = new DynamicJsonValue
            {
                ["Name"] = "John Doe",
                ["ContactInfoId"] = new DynamicJsonArray(new[]
                {
                    GetNestedObjectArray("foo", identityPartsSeparator),
                    GetNestedObjectArray("bar", identityPartsSeparator),
                    GetNestedObjectArray("foobar", identityPartsSeparator)
                })
            };

            using (var context = JsonOperationContext.ShortTermSingleUse())
            using (var reader = context.ReadObject(obj, "foo"))
            {
                var ids = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                IncludeUtil.GetDocIdFromInclude(reader, "ContactInfoId[].[].Bar.Foo", ids, identityPartsSeparator);
                Assert.Equal(new[] { $"foo{identityPartsSeparator}1", $"foo{identityPartsSeparator}2", $"foo{identityPartsSeparator}3", $"bar{identityPartsSeparator}1", $"bar{identityPartsSeparator}2", $"bar{identityPartsSeparator}3", $"foobar{identityPartsSeparator}1", $"foobar{identityPartsSeparator}2", $"foobar{identityPartsSeparator}3" }, ids);

                ids.Clear();
                IncludeUtil.GetDocIdFromInclude(reader, $"ContactInfoId[].[].Bar.Foo(abc{identityPartsSeparator})", ids, identityPartsSeparator);
                Assert.Equal(new[] { $"abc{identityPartsSeparator}foo{identityPartsSeparator}1", $"foo{identityPartsSeparator}1",
                    $"abc{identityPartsSeparator}foo{identityPartsSeparator}2", $"foo{identityPartsSeparator}2",
                    $"abc{identityPartsSeparator}foo{identityPartsSeparator}3", $"foo{identityPartsSeparator}3",
                    $"abc{identityPartsSeparator}bar{identityPartsSeparator}1", $"bar{identityPartsSeparator}1",
                    $"abc{identityPartsSeparator}bar{identityPartsSeparator}2", $"bar{identityPartsSeparator}2",
                    $"abc{identityPartsSeparator}bar{identityPartsSeparator}3", $"bar{identityPartsSeparator}3",
                    $"abc{identityPartsSeparator}foobar{identityPartsSeparator}1", $"foobar{identityPartsSeparator}1",
                    $"abc{identityPartsSeparator}foobar{identityPartsSeparator}2", $"foobar{identityPartsSeparator}2",
                    $"abc{identityPartsSeparator}foobar{identityPartsSeparator}3", $"foobar{identityPartsSeparator}3" }, ids);

                ids.Clear();
                IncludeUtil.GetDocIdFromInclude(reader, $"ContactInfoId[].[].Bar.Foo[{{0}}{identityPartsSeparator}abc]", ids, identityPartsSeparator);
                Assert.Equal(new[] { $"foo{identityPartsSeparator}1{identityPartsSeparator}abc", $"foo{identityPartsSeparator}1",
                    $"foo{identityPartsSeparator}2{identityPartsSeparator}abc", $"foo{identityPartsSeparator}2",
                    $"foo{identityPartsSeparator}3{identityPartsSeparator}abc", $"foo{identityPartsSeparator}3",
                    $"bar{identityPartsSeparator}1{identityPartsSeparator}abc", $"bar{identityPartsSeparator}1",
                    $"bar{identityPartsSeparator}2{identityPartsSeparator}abc", $"bar{identityPartsSeparator}2",
                    $"bar{identityPartsSeparator}3{identityPartsSeparator}abc", $"bar{identityPartsSeparator}3",
                    $"foobar{identityPartsSeparator}1{identityPartsSeparator}abc", $"foobar{identityPartsSeparator}1",
                    $"foobar{identityPartsSeparator}2{identityPartsSeparator}abc", $"foobar{identityPartsSeparator}2",
                    $"foobar{identityPartsSeparator}3{identityPartsSeparator}abc", $"foobar{identityPartsSeparator}3" }, ids);
            }
        }

        [Theory]
        [InlineData('/')]
        [InlineData(':')]
        public void FindDocIdFromPath_with_array_of_with_of_arrays_of_nested_objects_should_work2(char identityPartsSeparator)
        {
            var obj = new DynamicJsonValue
            {
                ["Name"] = "John Doe",
                ["ContactInfoId"] = new DynamicJsonArray(new[]
                {
                    new DynamicJsonValue
                    {
                        ["Foo"] = GetNestedObjectArray("foo", identityPartsSeparator)
                    },
                    new DynamicJsonValue
                    {
                        ["Foo"] = GetNestedObjectArray("bar", identityPartsSeparator)
                    },
                    new DynamicJsonValue
                    {
                        ["Foo"] = GetNestedObjectArray("foobar", identityPartsSeparator)
                    },
                })
            };

            using (var context = JsonOperationContext.ShortTermSingleUse())
            using (var reader = context.ReadObject(obj, "foo"))
            {
                var ids = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                IncludeUtil.GetDocIdFromInclude(reader, "ContactInfoId[].Foo[].Bar.Foo", ids, identityPartsSeparator);
                Assert.Equal(new[] { $"foo{identityPartsSeparator}1", $"foo{identityPartsSeparator}2", $"foo{identityPartsSeparator}3", $"bar{identityPartsSeparator}1", $"bar{identityPartsSeparator}2", $"bar{identityPartsSeparator}3", $"foobar{identityPartsSeparator}1", $"foobar{identityPartsSeparator}2", $"foobar{identityPartsSeparator}3" }, ids);

                ids.Clear();
                IncludeUtil.GetDocIdFromInclude(reader, $"ContactInfoId[].Foo[].Bar.Foo(abc{identityPartsSeparator})", ids, identityPartsSeparator);
                Assert.Equal(new[] { $"abc{identityPartsSeparator}foo{identityPartsSeparator}1", $"foo{identityPartsSeparator}1",
                    $"abc{identityPartsSeparator}foo{identityPartsSeparator}2", $"foo{identityPartsSeparator}2",
                    $"abc{identityPartsSeparator}foo{identityPartsSeparator}3", $"foo{identityPartsSeparator}3",
                    $"abc{identityPartsSeparator}bar{identityPartsSeparator}1", $"bar{identityPartsSeparator}1",
                    $"abc{identityPartsSeparator}bar{identityPartsSeparator}2", $"bar{identityPartsSeparator}2",
                    $"abc{identityPartsSeparator}bar{identityPartsSeparator}3", $"bar{identityPartsSeparator}3",
                    $"abc{identityPartsSeparator}foobar{identityPartsSeparator}1", $"foobar{identityPartsSeparator}1",
                    $"abc{identityPartsSeparator}foobar{identityPartsSeparator}2", $"foobar{identityPartsSeparator}2",
                    $"abc{identityPartsSeparator}foobar{identityPartsSeparator}3", $"foobar{identityPartsSeparator}3" }, ids);

                ids.Clear();
                IncludeUtil.GetDocIdFromInclude(reader, $"ContactInfoId[].Foo[].Bar.Foo[{{0}}{identityPartsSeparator}abc]", ids, identityPartsSeparator);
                Assert.Equal(new[] { $"foo{identityPartsSeparator}1{identityPartsSeparator}abc", $"foo{identityPartsSeparator}1",
                    $"foo{identityPartsSeparator}2{identityPartsSeparator}abc", $"foo{identityPartsSeparator}2",
                    $"foo{identityPartsSeparator}3{identityPartsSeparator}abc", $"foo{identityPartsSeparator}3",
                    $"bar{identityPartsSeparator}1{identityPartsSeparator}abc", $"bar{identityPartsSeparator}1",
                    $"bar{identityPartsSeparator}2{identityPartsSeparator}abc", $"bar{identityPartsSeparator}2",
                    $"bar{identityPartsSeparator}3{identityPartsSeparator}abc", $"bar{identityPartsSeparator}3",
                    $"foobar{identityPartsSeparator}1{identityPartsSeparator}abc", $"foobar{identityPartsSeparator}1",
                    $"foobar{identityPartsSeparator}2{identityPartsSeparator}abc", $"foobar{identityPartsSeparator}2",
                    $"foobar{identityPartsSeparator}3{identityPartsSeparator}abc", $"foobar{identityPartsSeparator}3" }, ids);
            }
        }

        [Theory]
        [InlineData('/')]
        [InlineData(':')]
        public void FindDocIdFromPath_with_array_of_with_of_arrays_of_nested_objects_should_work3(char identityPartsSeparator)
        {
            var obj = new DynamicJsonValue
            {
                ["Name"] = "John Doe",
                ["ContactInfoId"] = new DynamicJsonArray(new[]
                {
                    new DynamicJsonValue
                    {
                        ["BarX"] = new DynamicJsonValue
                        {
                            ["Foo"] = GetNestedObjectArray("foo", identityPartsSeparator)
                        }
                    },
                    new DynamicJsonValue
                    {
                        ["BarX"] = new DynamicJsonValue
                        {
                            ["Foo"] = GetNestedObjectArray("bar", identityPartsSeparator)
                        }
                    },
                    new DynamicJsonValue
                    {
                        ["BarX"] = new DynamicJsonValue
                        {
                            ["Foo"] = GetNestedObjectArray("foobar", identityPartsSeparator)
                        }
                    },
                })
            };

            using (var context = JsonOperationContext.ShortTermSingleUse())
            using (var reader = context.ReadObject(obj, "foo"))
            {
                var ids = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                IncludeUtil.GetDocIdFromInclude(reader, "ContactInfoId[].BarX.Foo[].Bar.Foo", ids, identityPartsSeparator);
                Assert.Equal(new[] { $"foo{identityPartsSeparator}1", $"foo{identityPartsSeparator}2", $"foo{identityPartsSeparator}3", $"bar{identityPartsSeparator}1", $"bar{identityPartsSeparator}2", $"bar{identityPartsSeparator}3", $"foobar{identityPartsSeparator}1", $"foobar{identityPartsSeparator}2", $"foobar{identityPartsSeparator}3" }, ids);

                ids.Clear();
                IncludeUtil.GetDocIdFromInclude(reader, $"ContactInfoId[].BarX.Foo[].Bar.Foo(abc{identityPartsSeparator})", ids, identityPartsSeparator);
                Assert.Equal(new[] { $"abc{identityPartsSeparator}foo{identityPartsSeparator}1", $"foo{identityPartsSeparator}1",
                    $"abc{identityPartsSeparator}foo{identityPartsSeparator}2", $"foo{identityPartsSeparator}2",
                    $"abc{identityPartsSeparator}foo{identityPartsSeparator}3", $"foo{identityPartsSeparator}3",
                    $"abc{identityPartsSeparator}bar{identityPartsSeparator}1", $"bar{identityPartsSeparator}1",
                    $"abc{identityPartsSeparator}bar{identityPartsSeparator}2", $"bar{identityPartsSeparator}2",
                    $"abc{identityPartsSeparator}bar{identityPartsSeparator}3", $"bar{identityPartsSeparator}3",
                    $"abc{identityPartsSeparator}foobar{identityPartsSeparator}1", $"foobar{identityPartsSeparator}1",
                    $"abc{identityPartsSeparator}foobar{identityPartsSeparator}2", $"foobar{identityPartsSeparator}2",
                    $"abc{identityPartsSeparator}foobar{identityPartsSeparator}3", $"foobar{identityPartsSeparator}3" }, ids);

                ids.Clear();
                IncludeUtil.GetDocIdFromInclude(reader, $"ContactInfoId[].BarX.Foo[].Bar.Foo[{{0}}{identityPartsSeparator}abc]", ids, identityPartsSeparator);
                Assert.Equal(new[] { $"foo{identityPartsSeparator}1{identityPartsSeparator}abc", $"foo{identityPartsSeparator}1",
                    $"foo{identityPartsSeparator}2{identityPartsSeparator}abc", $"foo{identityPartsSeparator}2",
                    $"foo{identityPartsSeparator}3{identityPartsSeparator}abc", $"foo{identityPartsSeparator}3",
                    $"bar{identityPartsSeparator}1{identityPartsSeparator}abc", $"bar{identityPartsSeparator}1",
                    $"bar{identityPartsSeparator}2{identityPartsSeparator}abc", $"bar{identityPartsSeparator}2",
                    $"bar{identityPartsSeparator}3{identityPartsSeparator}abc", $"bar{identityPartsSeparator}3",
                    $"foobar{identityPartsSeparator}1{identityPartsSeparator}abc", $"foobar{identityPartsSeparator}1",
                    $"foobar{identityPartsSeparator}2{identityPartsSeparator}abc", $"foobar{identityPartsSeparator}2",
                    $"foobar{identityPartsSeparator}3{identityPartsSeparator}abc", $"foobar{identityPartsSeparator}3" }, ids);
            }
        }

        private static DynamicJsonArray GetStringArray(string prefix, char identityPartsSeparator)
        {
            return new DynamicJsonArray(new[] { $"{prefix}{identityPartsSeparator}1", $"{prefix}{identityPartsSeparator}2", $"{prefix}{identityPartsSeparator}3" });
        }

        private static DynamicJsonArray GetObjectArray(string prefix, char identityPartsSeparator)
        {
            return new DynamicJsonArray(new[]
            {
                new DynamicJsonValue
                {
                    ["Foo"] = $"{prefix}{identityPartsSeparator}1"
                },
                new DynamicJsonValue
                {
                    ["Foo"] = $"{prefix}{identityPartsSeparator}2"
                },
                new DynamicJsonValue
                {
                    ["Foo"] = $"{prefix}{identityPartsSeparator}3"
                },
            });
        }

        private static DynamicJsonArray GetNestedObjectArray(string prefix, char identityPartsSeparator)
        {
            return new DynamicJsonArray(new[]
            {
                new DynamicJsonValue
                {
                    ["Bar"] = new DynamicJsonValue
                    {
                        ["Foo"] = $"{prefix}{identityPartsSeparator}1"
                    }
                },
                new DynamicJsonValue
                {
                    ["Bar"] = new DynamicJsonValue
                    {
                        ["Foo"] = $"{prefix}{identityPartsSeparator}2"
                    }
                },
                new DynamicJsonValue
                {
                    ["Bar"] = new DynamicJsonValue
                    {
                        ["Foo"] = $"{prefix}{identityPartsSeparator}3"
                    }
                },
            });
        }

        [Fact]
        public void FindDocIdFromPath_with_array_of_nested_objects_should_work2()
        {
            var obj = new DynamicJsonValue
            {
                ["Name"] = "John Doe",
                ["Foo"] = new DynamicJsonValue
                {
                    ["Bar"] = new DynamicJsonValue
                    {
                        ["C"] = new DynamicJsonArray(new[]
                        {
                            new DynamicJsonValue
                            {
                                ["A"] = new DynamicJsonValue
                                {
                                    ["X"] = new DynamicJsonValue
                                    {
                                        ["Y"] = "ccc/1"
                                    }
                                }
                            },
                            new DynamicJsonValue
                            {
                                ["A"] = new DynamicJsonValue
                                {
                                    ["X"] = new DynamicJsonValue
                                    {
                                        ["YABC"] = "ccc/2"
                                    }
                                }
                            },
                            new DynamicJsonValue
                            {
                                ["A"] = new DynamicJsonValue
                                {
                                    ["X"] = new DynamicJsonValue
                                    {
                                        ["Y"] = "ccc/3"
                                    }
                                }
                            },
                            new DynamicJsonValue
                            {
                                ["A"] = new DynamicJsonValue
                                {
                                    ["X"] = new DynamicJsonValue
                                    {
                                        ["Y"] = "ccc/4"
                                    }
                                }
                            },
                            new DynamicJsonValue
                            {
                                ["A"] = new DynamicJsonValue
                                {
                                    ["X"] = new DynamicJsonValue
                                    {
                                        ["Y"] = "ccc/5"
                                    }
                                }
                            },
                        })
                    }
                }
            };
            using (var context = JsonOperationContext.ShortTermSingleUse())
            using (var reader = context.ReadObject(obj, "foo"))
            {
                var ids = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                IncludeUtil.GetDocIdFromInclude(reader, "Foo.Bar.C[].A.X.Y", ids, '/');
                Assert.Equal(new[] { "ccc/1", "ccc/3", "ccc/4", "ccc/5" }, ids);
            }
        }


        [Fact]
        public void FindDocIdFromPath_with_array_of_nested_objects_with_prefix_should_work()
        {
            var obj = new DynamicJsonValue
            {
                ["Name"] = "John Doe",
                ["Foo"] = new DynamicJsonValue
                {
                    ["Bar"] = new DynamicJsonValue
                    {
                        ["C"] = new DynamicJsonArray(new[]
                        {
                            new DynamicJsonValue
                            {
                                ["A"] = new DynamicJsonValue
                                {
                                    ["X"] = new DynamicJsonValue
                                    {
                                        ["Y"] = 1
                                    }
                                }
                            },
                            new DynamicJsonValue
                            {
                                ["A"] = new DynamicJsonValue
                                {
                                    ["X"] = new DynamicJsonValue
                                    {
                                        ["YABC"] = 2
                                    }
                                }
                            },
                            new DynamicJsonValue
                            {
                                ["A"] = new DynamicJsonValue
                                {
                                    ["X"] = new DynamicJsonValue
                                    {
                                        ["Y"] = 3
                                    }
                                }
                            },
                            new DynamicJsonValue
                            {
                                ["A"] = new DynamicJsonValue
                                {
                                    ["X"] = new DynamicJsonValue
                                    {
                                        ["Y"] = 4
                                    }
                                }
                            },
                            new DynamicJsonValue
                            {
                                ["A"] = new DynamicJsonValue
                                {
                                    ["X"] = new DynamicJsonValue
                                    {
                                        ["Y"] = 5
                                    }
                                }
                            },
                        })
                    }
                }
            };
            using (var context = JsonOperationContext.ShortTermSingleUse())
            using (var reader = context.ReadObject(obj, "foo"))
            {
                var ids = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                IncludeUtil.GetDocIdFromInclude(reader, "Foo.Bar.C[].A.X.Y(ccc/)", ids, '/');
                Assert.Equal(new[] { "ccc/1", "1", "ccc/3", "3", "ccc/4", "4", "ccc/5", "5" }, ids);
            }
        }

        [Fact]
        public void FindDocIdFromPath_with_array_of_nested_objects_with_suffix_should_work()
        {
            var obj = new DynamicJsonValue
            {
                ["Name"] = "John Doe",
                ["Foo"] = new DynamicJsonValue
                {
                    ["Bar"] = new DynamicJsonValue
                    {
                        ["C"] = new DynamicJsonArray(new[]
                        {
                            new DynamicJsonValue
                            {
                                ["A"] = new DynamicJsonValue
                                {
                                    ["X"] = new DynamicJsonValue
                                    {
                                        ["Y"] = 1
                                    }
                                }
                            },
                            new DynamicJsonValue
                            {
                                ["A"] = new DynamicJsonValue
                                {
                                    ["X"] = new DynamicJsonValue
                                    {
                                        ["YABC"] = 2
                                    }
                                }
                            },
                            new DynamicJsonValue
                            {
                                ["A"] = new DynamicJsonValue
                                {
                                    ["X"] = new DynamicJsonValue
                                    {
                                        ["Y"] = 3
                                    }
                                }
                            },
                            new DynamicJsonValue
                            {
                                ["A"] = new DynamicJsonValue
                                {
                                    ["X"] = new DynamicJsonValue
                                    {
                                        ["Y"] = 4
                                    }
                                }
                            },
                            new DynamicJsonValue
                            {
                                ["A"] = new DynamicJsonValue
                                {
                                    ["X"] = new DynamicJsonValue
                                    {
                                        ["Y"] = 5
                                    }
                                }
                            },
                        })
                    }
                }
            };
            using (var context = JsonOperationContext.ShortTermSingleUse())
            using (var reader = context.ReadObject(obj, "foo"))
            {
                var ids = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                IncludeUtil.GetDocIdFromInclude(reader, "Foo.Bar.C[].A.X.Y[{0}/ccc]", ids, '/');
                Assert.Equal(new[] { "1/ccc", "1", "3/ccc", "3", "4/ccc", "4", "5/ccc", "5" }, ids);
            }
        }

        [Fact]
        public void FindDocIdFromPath_with_array_selection_should_work_in_flat_object_with_prefix1()
        {
            var obj = new DynamicJsonValue
            {
                ["Name"] = "John Doe",
                ["ContactInfoId"] = new DynamicJsonArray(new object[] { 1, 2, 3 })
            };
            using (var context = JsonOperationContext.ShortTermSingleUse())
            using (var reader = context.ReadObject(obj, "foo"))
            {
                var ids = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                IncludeUtil.GetDocIdFromInclude(reader, "ContactInfoId[].(foo/)", ids, '/');
                Assert.Equal(new[] { "foo/1", "1", "foo/2", "2", "foo/3", "3" }, ids);
            }
        }

        [Fact]
        public void FindDocIdFromPath_with_array_selection_should_work_in_flat_object_with_suffix1()
        {
            var obj = new DynamicJsonValue
            {
                ["Name"] = "John Doe",
                ["ContactInfoId"] = new DynamicJsonArray(new object[] { 1, 2, 3 })
            };
            using (var context = JsonOperationContext.ShortTermSingleUse())
            using (var reader = context.ReadObject(obj, "foo"))
            {
                var ids = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                IncludeUtil.GetDocIdFromInclude(reader, "ContactInfoId[].[{0}/foo]", ids, '/');
                Assert.Equal(new[] { "1/foo", "1", "2/foo", "2", "3/foo", "3" }, ids);
            }
        }

        [Fact]
        public void FindDocIdFromPath_with_array_selection_should_work_in_flat_object_with_prefix2()
        {
            var obj = new DynamicJsonValue
            {
                ["Name"] = "John Doe",
                ["ContactInfoId"] = new DynamicJsonArray(new[]
                {
                    new DynamicJsonValue
                    {
                        ["Foo"] = 11
                    },
                    new DynamicJsonValue
                    {
                        ["Foo"] = 2
                    },
                    new DynamicJsonValue
                    {
                        ["Foo"] = 3
                    },
                    new DynamicJsonValue
                    {
                        ["Foo"] = 4
                    },
                })
            };
            using (var context = JsonOperationContext.ShortTermSingleUse())
            using (var reader = context.ReadObject(obj, "foo"))
            {
                var ids = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                IncludeUtil.GetDocIdFromInclude(reader, "ContactInfoId[].Foo(foo/)", ids, '/');
                Assert.Equal(new[] { "foo/11", "11", "foo/2", "2", "foo/3", "3", "foo/4", "4" }, ids);
            }
        }

        [Fact]
        public void FindDocIdFromPath_with_array_selection_should_work_in_flat_object_with_suffix2()
        {
            var obj = new DynamicJsonValue
            {
                ["Name"] = "John Doe",
                ["ContactInfoId"] = new DynamicJsonArray(new[]
                {
                    new DynamicJsonValue
                    {
                        ["Foo"] = 11
                    },
                    new DynamicJsonValue
                    {
                        ["Foo"] = 2
                    },
                    new DynamicJsonValue
                    {
                        ["Foo"] = 3
                    },
                    new DynamicJsonValue
                    {
                        ["Foo"] = 4
                    },
                })
            };
            using (var context = JsonOperationContext.ShortTermSingleUse())
            using (var reader = context.ReadObject(obj, "foo"))
            {
                var ids = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                IncludeUtil.GetDocIdFromInclude(reader, "ContactInfoId[].Foo[{0}/foo]", ids, '/');
                Assert.Equal(new[] { "11/foo", "11", "2/foo", "2", "3/foo", "3", "4/foo", "4" }, ids);
            }
        }

        [Fact]
        public void FindDocIdFromPath_with_array_selection_should_work_in_flat_object1()
        {
            var obj = new DynamicJsonValue
            {
                ["Name"] = "John Doe",
                ["ContactInfoId"] = new DynamicJsonArray(new[] { "foo/1", "foo/2", "foo/3" })
            };
            using (var context = JsonOperationContext.ShortTermSingleUse())
            using (var reader = context.ReadObject(obj, "foo"))
            {
                var ids = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                IncludeUtil.GetDocIdFromInclude(reader, "ContactInfoId[].", ids, '/');
                Assert.Equal(new[] { "foo/1", "foo/2", "foo/3" }, ids);
            }
        }

        [Fact]
        public void FindDocIdFromPath_with_array_selection_should_work_in_nested_object2()
        {
            var obj = new DynamicJsonValue
            {
                ["Name"] = "John Doe",
                ["Foo"] = new DynamicJsonValue
                {
                    ["Bar"] = new DynamicJsonValue
                    {
                        ["ContactInfoId"] = new DynamicJsonArray(new[] { "foo/1", "foo/2", "foo/3" })
                    },
                    ["ContactInfoId2"] = new DynamicJsonArray(new[] { "foo/1", "foo/2", "foo/3" })
                }
            };
            using (var context = JsonOperationContext.ShortTermSingleUse())
            using (var reader = context.ReadObject(obj, "foo"))
            {
                var ids = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                IncludeUtil.GetDocIdFromInclude(reader, "Foo.Bar.ContactInfoId[].", ids, '/');
                Assert.Equal(new[] { "foo/1", "foo/2", "foo/3" }, ids);

                ids.Clear();
                IncludeUtil.GetDocIdFromInclude(reader, "Foo.ContactInfoId2[].", ids, '/');
                Assert.Equal(new[] { "foo/1", "foo/2", "foo/3" }, ids);
            }
        }

        [Fact]
        public void FindDocIdFromPath_with_array_selection_should_work_in_flat_object2()
        {
            var obj = new DynamicJsonValue
            {
                ["Name"] = "John Doe",
                ["ContactInfoId1"] = new DynamicJsonArray(new object[] { 1, 2, 3 }),
                ["ContactInfoId2"] = new DynamicJsonArray(new object[] { 1.1, 2.2, 3.3 }),
                ["ContactInfoId3"] = new DynamicJsonArray(new object[] { (long)1, (long)2, (long)3 })
            };
            using (var context = JsonOperationContext.ShortTermSingleUse())
            using (var reader = context.ReadObject(obj, "foo"))
            {
                var ids = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                IncludeUtil.GetDocIdFromInclude(reader, "ContactInfoId1[].", ids, '/');
                Assert.Equal(new object[] { "1", "2", "3" }, ids);

                ids.Clear();
                IncludeUtil.GetDocIdFromInclude(reader, "ContactInfoId2[].", ids, '/');
                Assert.Equal(new object[] { "1.1", "2.2", "3.3" }, ids);

                ids.Clear();
                IncludeUtil.GetDocIdFromInclude(reader, "ContactInfoId3[].", ids, '/');
                Assert.Equal(new object[] { "1", "2", "3" }, ids);
            }
        }

        [Fact]
        public void FindDocIdFromPath_should_return_value_for_single_level_nested_path()
        {
            var obj = new DynamicJsonValue
            {
                ["Name"] = "John Doe",
                ["ExtendedInfo"] = new DynamicJsonValue
                {
                    ["ContactInfoId"] = "contacts/1"
                }
            };
            using (var context = JsonOperationContext.ShortTermSingleUse())
            using (var reader = context.ReadObject(obj, "foo"))
            {
                var ids = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                IncludeUtil.GetDocIdFromInclude(reader, "ExtendedInfo.ContactInfoId", ids, '/');
                Assert.Equal("contacts/1", ids.FirstOrDefault());

            }
        }

        [Fact]
        public void FindDocIdFromPath_should_return_value_for_single_level_nested_path_with_prefix()
        {
            var obj = new DynamicJsonValue
            {
                ["Name"] = "John Doe",
                ["ExtendedInfo"] = new DynamicJsonValue
                {
                    ["ContactInfoId"] = 1
                }
            };
            using (var context = JsonOperationContext.ShortTermSingleUse())
            using (var reader = context.ReadObject(obj, "foo"))
            {
                var ids = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                IncludeUtil.GetDocIdFromInclude(reader, "ExtendedInfo.ContactInfoId(contacts/)", ids, '/');
                Assert.Equal("contacts/1", ids.FirstOrDefault());

                //edge cases
                ids.Clear();
                IncludeUtil.GetDocIdFromInclude(reader, "ExtendedInfo.ContactInfoId()", ids, '/');
                Assert.Equal("1", ids.FirstOrDefault());

                ids.Clear();
                IncludeUtil.GetDocIdFromInclude(reader, "ExtendedInfo.ContactInfoId(c/)", ids, '/');
                Assert.Equal("c/1", ids.FirstOrDefault());

                ids.Clear();
                IncludeUtil.GetDocIdFromInclude(reader, "ExtendedInfo.ContactInfoId(ca/)", ids, '/');
                Assert.Equal("ca/1", ids.FirstOrDefault());

                ids.Clear();
                IncludeUtil.GetDocIdFromInclude(reader, "ExtendedInfo.ContactInfoId(/)", ids, '/');
                Assert.Equal("/1", ids.FirstOrDefault());
            }
        }

        [Fact]
        public void FindDocIdFromPath_should_return_value_for_single_level_nested_path_with_suffix()
        {
            var obj = new DynamicJsonValue
            {
                ["Name"] = "John Doe",
                ["ExtendedInfo"] = new DynamicJsonValue
                {
                    ["ContactInfoId"] = 1
                }
            };
            using (var context = JsonOperationContext.ShortTermSingleUse())
            using (var reader = context.ReadObject(obj, "foo"))
            {
                var ids = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                IncludeUtil.GetDocIdFromInclude(reader, "ExtendedInfo.ContactInfoId[{0}/contacts]", ids, '/');
                Assert.Equal("1/contacts", ids.FirstOrDefault());

                //edge cases
                ids.Clear();
                IncludeUtil.GetDocIdFromInclude(reader, "ExtendedInfo.ContactInfoId[]", ids, '/');
                Assert.Equal("1", ids.FirstOrDefault());

                ids.Clear();
                IncludeUtil.GetDocIdFromInclude(reader, "ExtendedInfo.ContactInfoId[{0}/c]", ids, '/');
                Assert.Equal("1/c", ids.FirstOrDefault());

                ids.Clear();
                IncludeUtil.GetDocIdFromInclude(reader, "ExtendedInfo.ContactInfoId[{0}/ca]", ids, '/');
                Assert.Equal("1/ca", ids.FirstOrDefault());

                ids.Clear();
                IncludeUtil.GetDocIdFromInclude(reader, "ExtendedInfo.ContactInfoId[{0}/]", ids, '/');
                Assert.Equal("1/", ids.FirstOrDefault());
            }
        }

        [Fact]
        public void FindDocIdFromPath_should_return_value_for_single_level_nested_path_with_prefix_and_string_value()
        {
            var obj = new DynamicJsonValue
            {
                ["Name"] = "John Doe",
                ["ExtendedInfo"] = new DynamicJsonValue
                {
                    ["ContactInfoId"] = "megadevice"
                }
            };
            using (var context = JsonOperationContext.ShortTermSingleUse())
            using (var reader = context.ReadObject(obj, "foo"))
            {
                var ids = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                IncludeUtil.GetDocIdFromInclude(reader, "ExtendedInfo.ContactInfoId(contacts/)", ids, '/');
                Assert.Equal("contacts/megadevice", ids.FirstOrDefault());

                //edge cases
                ids.Clear();
                IncludeUtil.GetDocIdFromInclude(reader, "ExtendedInfo.ContactInfoId()", ids, '/');
                Assert.Equal("megadevice", ids.FirstOrDefault());

                ids.Clear();
                IncludeUtil.GetDocIdFromInclude(reader, "ExtendedInfo.ContactInfoId(c/)", ids, '/');
                Assert.Equal("c/megadevice", ids.FirstOrDefault());

                ids.Clear();
                IncludeUtil.GetDocIdFromInclude(reader, "ExtendedInfo.ContactInfoId(ca/)", ids, '/');
                Assert.Equal("ca/megadevice", ids.FirstOrDefault());

                ids.Clear();
                IncludeUtil.GetDocIdFromInclude(reader, "ExtendedInfo.ContactInfoId(/)", ids, '/');
                Assert.Equal("/megadevice", ids.FirstOrDefault());
            }
        }

        [Fact]
        public void FindDocIdFromPath_should_return_value_for_single_level_nested_path_with_suffix_and_string_value()
        {
            var obj = new DynamicJsonValue
            {
                ["Name"] = "John Doe",
                ["ExtendedInfo"] = new DynamicJsonValue
                {
                    ["ContactInfoId"] = "megadevice"
                }
            };
            using (var context = JsonOperationContext.ShortTermSingleUse())
            using (var reader = context.ReadObject(obj, "foo"))
            {
                var ids = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                IncludeUtil.GetDocIdFromInclude(reader, "ExtendedInfo.ContactInfoId[{0}/contacts]", ids, '/');
                Assert.Equal("megadevice/contacts", ids.FirstOrDefault());

                //edge cases
                ids.Clear();
                IncludeUtil.GetDocIdFromInclude(reader, "ExtendedInfo.ContactInfoId[]", ids, '/');
                Assert.Equal("megadevice", ids.FirstOrDefault());

                ids.Clear();
                IncludeUtil.GetDocIdFromInclude(reader, "ExtendedInfo.ContactInfoId[{0}/c]", ids, '/');
                Assert.Equal("megadevice/c", ids.FirstOrDefault());

                ids.Clear();
                IncludeUtil.GetDocIdFromInclude(reader, "ExtendedInfo.ContactInfoId[{0}/ca]", ids, '/');
                Assert.Equal("megadevice/ca", ids.FirstOrDefault());

                ids.Clear();
                IncludeUtil.GetDocIdFromInclude(reader, "ExtendedInfo.ContactInfoId[{0}/]", ids, '/');
                Assert.Equal("megadevice/", ids.FirstOrDefault());
            }
        }

        [Fact]
        public void FindDocIdFromPath_should_return_value_for_multiple_level_nested_path()
        {
            var obj = new DynamicJsonValue
            {
                ["Name"] = "John Doe",
                ["ExtendedInfo1"] = new DynamicJsonValue
                {
                    ["ExtendedInfo2"] = new DynamicJsonValue
                    {
                        ["AdressInfo"] = "address/1",
                        ["ExtendedInfo3"] = new DynamicJsonValue
                        {
                            ["ContactInfoId1"] = "contacts/1",
                            ["ContactInfoId2"] = "contacts/2"
                        }
                    }
                }
            };
            using (var context = JsonOperationContext.ShortTermSingleUse())
            using (var reader = context.ReadObject(obj, "foo"))
            {
                var ids = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                IncludeUtil.GetDocIdFromInclude(reader, "ExtendedInfo1.ExtendedInfo2.ExtendedInfo3.ContactInfoId1", ids, '/');
                Assert.Equal("contacts/1", ids.FirstOrDefault());

                ids.Clear();
                IncludeUtil.GetDocIdFromInclude(reader, "ExtendedInfo1.ExtendedInfo2.ExtendedInfo3.ContactInfoId2", ids, '/');
                Assert.Equal("contacts/2", ids.FirstOrDefault());

                ids.Clear();
                IncludeUtil.GetDocIdFromInclude(reader, "ExtendedInfo1.ExtendedInfo2.AdressInfo", ids, '/');
                Assert.Equal("address/1", ids.FirstOrDefault());
            }
        }


        [Fact]
        public void FindDocIdFromPath_should_return_empty_for_incorrect_path()
        {
            var obj = new DynamicJsonValue
            {
                ["Name"] = "John Doe",
                ["ContactInfoId"] = "contacts/1"
            };
            using (var context = JsonOperationContext.ShortTermSingleUse())
            using (var reader = context.ReadObject(obj, "foo"))
            {
                var ids = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                IncludeUtil.GetDocIdFromInclude(reader, "ExtendedInfo.ContactInfoId", ids, '/');

                Assert.Empty(ids);
            }
        }

        [Fact]
        public void FindDocIdFromPath_should_work_in_flat_object1()
        {
            var obj = new DynamicJsonValue
            {
                ["Name"] = "John Doe",
                ["ContactInfoId"] = "contacts/1"
            };
            using (var context = JsonOperationContext.ShortTermSingleUse())
            using (var reader = context.ReadObject(obj, "foo"))
            {
                var ids = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                IncludeUtil.GetDocIdFromInclude(reader, "ContactInfoId", ids, '/');
                Assert.Equal("contacts/1", ids.FirstOrDefault());
            }
        }

        [Fact]
        public void FindDocIdFromPath_should_work_in_flat_object2()
        {
            var obj = new DynamicJsonValue
            {
                ["Name"] = "John Doe",
                //for numbers, only int32, int64 and double are supported
                ["ContactInfoId"] = 12,
                ["ContactInfoId2"] = (long)56,
                ["ContactInfoId3"] = 78.89, //this one is double
            };
            using (var context = JsonOperationContext.ShortTermSingleUse())
            using (var reader = context.ReadObject(obj, "foo"))
            {
                var ids = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                IncludeUtil.GetDocIdFromInclude(reader, "ContactInfoId", ids, '/');
                Assert.Equal("12", ids.FirstOrDefault());

                ids.Clear();
                IncludeUtil.GetDocIdFromInclude(reader, "ContactInfoId2", ids, '/');
                Assert.Equal("56", ids.FirstOrDefault());

                ids.Clear();
                IncludeUtil.GetDocIdFromInclude(reader, "ContactInfoId3", ids, '/');
                Assert.Equal("78.89", ids.FirstOrDefault());
            }
        }

        [Fact]
        public void FindDocIdFromPath_should_return_empty_with_incomplete_prefix_in_flat_object()
        {
            var obj = new DynamicJsonValue
            {
                ["Name"] = "John Doe",
                ["ContactInfoId"] = 1
            };
            using (var context = JsonOperationContext.ShortTermSingleUse())
            using (var reader = context.ReadObject(obj, "foo"))
            {
                var ids = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

                IncludeUtil.GetDocIdFromInclude(reader, "ContactInfoId(contacts/", ids, '/');
                Assert.Empty(ids);
            }
        }

        [Fact]
        public void FindDocIdFromPath_should_return_empty_with_incomplete_suffix_in_flat_object()
        {
            var obj = new DynamicJsonValue
            {
                ["Name"] = "John Doe",
                ["ContactInfoId"] = 1
            };
            using (var context = JsonOperationContext.ShortTermSingleUse())
            using (var reader = context.ReadObject(obj, "foo"))
            {
                var ids = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

                IncludeUtil.GetDocIdFromInclude(reader, "ContactInfoId[{0}/contacts", ids, '/');
                Assert.Empty(ids);

                IncludeUtil.GetDocIdFromInclude(reader, "ContactInfoId[{0/contacts]", ids, '/');
                Assert.Empty(ids);

                IncludeUtil.GetDocIdFromInclude(reader, "ContactInfoId[{}/contacts]", ids, '/');
                Assert.Empty(ids);

                IncludeUtil.GetDocIdFromInclude(reader, "ContactInfoId[0}/contacts]", ids, '/');
                Assert.Empty(ids);
            }
        }

        [Fact]
        public void FindDocIdFromPath_should_work_with_prefix_in_flat_object()
        {
            var obj = new DynamicJsonValue
            {
                ["Name"] = "John Doe",
                ["ContactInfoId"] = 1
            };
            using (var context = JsonOperationContext.ShortTermSingleUse())
            using (var reader = context.ReadObject(obj, "foo"))
            {
                var ids = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

                IncludeUtil.GetDocIdFromInclude(reader, "ContactInfoId(contacts/)", ids, '/');
                Assert.Equal("contacts/1", ids.FirstOrDefault());
            }
        }

        [Fact]
        public void FindDocIdFromPath_should_work_with_suffix_in_flat_object()
        {
            var obj = new DynamicJsonValue
            {
                ["Name"] = "John Doe",
                ["ContactInfoId"] = 1
            };
            using (var context = JsonOperationContext.ShortTermSingleUse())
            using (var reader = context.ReadObject(obj, "foo"))
            {
                var ids = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

                IncludeUtil.GetDocIdFromInclude(reader, "ContactInfoId[{0}/contacts]", ids, '/');
                Assert.Equal("1/contacts", ids.FirstOrDefault());
            }
        }


        [Fact]
        public void FindDocIdFromPath_should_work_with_very_short_prefix_in_flat_object()
        {
            var obj = new DynamicJsonValue
            {
                ["Name"] = "John Doe",
                ["ContactInfoId"] = 1
            };
            using (var context = JsonOperationContext.ShortTermSingleUse())
            using (var reader = context.ReadObject(obj, "foo"))
            {
                var ids = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

                IncludeUtil.GetDocIdFromInclude(reader, "ContactInfoId(c/)", ids, '/');
                Assert.Equal("c/1", ids.FirstOrDefault());

                ids.Clear();
                IncludeUtil.GetDocIdFromInclude(reader, "ContactInfoId(ca/)", ids, '/');
                Assert.Equal("ca/1", ids.FirstOrDefault());

                ids.Clear();
                IncludeUtil.GetDocIdFromInclude(reader, "ContactInfoId(caa/)", ids, '/');
                Assert.Equal("caa/1", ids.FirstOrDefault());
            }
        }

        [Fact]
        public void FindDocIdFromPath_should_work_with_very_short_suffix_in_flat_object()
        {
            var obj = new DynamicJsonValue
            {
                ["Name"] = "John Doe",
                ["ContactInfoId"] = 1
            };
            using (var context = JsonOperationContext.ShortTermSingleUse())
            using (var reader = context.ReadObject(obj, "foo"))
            {
                var ids = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

                IncludeUtil.GetDocIdFromInclude(reader, "ContactInfoId[{0}/c]", ids, '/');
                Assert.Equal("1/c", ids.FirstOrDefault());

                ids.Clear();
                IncludeUtil.GetDocIdFromInclude(reader, "ContactInfoId[{0}/ca]", ids, '/');
                Assert.Equal("1/ca", ids.FirstOrDefault());

                ids.Clear();
                IncludeUtil.GetDocIdFromInclude(reader, "ContactInfoId[{0}/caa]", ids, '/');
                Assert.Equal("1/caa", ids.FirstOrDefault());
            }
        }

        [Fact]
        public void FindDocIdFromPath_with_multiple_targets_should_work_in_flat_object()
        {
            var obj = new DynamicJsonValue
            {
                ["Name"] = "John Doe",
                ["ContactInfoId"] = "contacts/1",
                ["AddressInfoId"] = "addresses/1",
                ["CarInfoId"] = "cars/1"
            };
            using (var context = JsonOperationContext.ShortTermSingleUse())
            using (var reader = context.ReadObject(obj, "foo"))
            {
                var ids = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

                IncludeUtil.GetDocIdFromInclude(reader, "AddressInfoId", ids, '/');
                Assert.Equal("addresses/1", ids.FirstOrDefault());

                ids.Clear();
                IncludeUtil.GetDocIdFromInclude(reader, "ContactInfoId", ids, '/');
                Assert.Equal("contacts/1", ids.FirstOrDefault());

                ids.Clear();
                IncludeUtil.GetDocIdFromInclude(reader, "CarInfoId", ids, '/');
                Assert.Equal("cars/1", ids.FirstOrDefault());
            }
        }
    }
}
