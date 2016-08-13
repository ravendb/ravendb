using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Server.Json;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Xunit;

namespace FastTests.Utils
{
    public class IncludeUtilTests
    {
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
                IncludeUtil.GetDocIdFromInclude(reader, "ContactInfoId,Foo", ids);
                Assert.Equal(new[] { "foobar/1", "foobar/2", "foobar/3", "foobar/4" }, ids);

                ids.Clear();
                IncludeUtil.GetDocIdFromInclude(reader, "Foo.Bar.C,X.Y", ids);
                Assert.Equal(new[] { "ccc/1", "ccc/2", "ccc/3", "ccc/5" }, ids);
            }
        }

        [Fact]
        public void FindDocIdFromPath_with_array_of_arrays_with_simple_values_should_work()
        {
            var obj = new DynamicJsonValue
            {
                ["Name"] = "John Doe",
                ["ContactInfoId"] = new DynamicJsonArray(new[]
                {
                    GetStringArray("foo"),
                    GetStringArray("bar"),
                    GetStringArray("foobar")
                })
            };

            using (var context = JsonOperationContext.ShortTermSingleUse())
            using (var reader = context.ReadObject(obj, "foo"))
            {
                var ids = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                IncludeUtil.GetDocIdFromInclude(reader, "ContactInfoId,,", ids);
                Assert.Equal(new[] { "foo/1", "foo/2", "foo/3", "bar/1", "bar/2", "bar/3", "foobar/1", "foobar/2", "foobar/3" }, ids);

                ids.Clear();
                IncludeUtil.GetDocIdFromInclude(reader, "ContactInfoId,,(abc/)", ids);
                Assert.Equal(new[] { "abc/foo/1", "foo/1",
                    "abc/foo/2", "foo/2",
                    "abc/foo/3", "foo/3",
                    "abc/bar/1", "bar/1",
                    "abc/bar/2", "bar/2",
                    "abc/bar/3", "bar/3",
                    "abc/foobar/1", "foobar/1",
                    "abc/foobar/2", "foobar/2",
                    "abc/foobar/3", "foobar/3" }, ids);

                ids.Clear();
                IncludeUtil.GetDocIdFromInclude(reader, "ContactInfoId,,[{0}/abc]", ids);
                Assert.Equal(new[] { "foo/1/abc", "foo/1",
                    "foo/2/abc", "foo/2",
                    "foo/3/abc", "foo/3",
                    "bar/1/abc", "bar/1",
                    "bar/2/abc", "bar/2",
                    "bar/3/abc", "bar/3",
                    "foobar/1/abc", "foobar/1",
                    "foobar/2/abc", "foobar/2",
                    "foobar/3/abc", "foobar/3" }, ids);
            }
        }

        [Fact]
        public void FindDocIdFromPath_with_array_of_arrays_of_objects_should_work()
        {
            var obj = new DynamicJsonValue
            {
                ["Name"] = "John Doe",
                ["ContactInfoId"] = new DynamicJsonArray(new[]
                {
                    GetObjectArray("foo"),
                    GetObjectArray("bar"),
                    GetObjectArray("foobar")
                })
            };

            using (var context = JsonOperationContext.ShortTermSingleUse())
            using (var reader = context.ReadObject(obj, "foo"))
            {
                var ids = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                IncludeUtil.GetDocIdFromInclude(reader, "ContactInfoId,,Foo", ids);
                Assert.Equal(new[] { "foo/1", "foo/2", "foo/3", "bar/1", "bar/2", "bar/3", "foobar/1", "foobar/2", "foobar/3" }, ids);

                ids.Clear();
                IncludeUtil.GetDocIdFromInclude(reader, "ContactInfoId,,Foo(abc/)", ids);
                Assert.Equal(new[] { "abc/foo/1", "foo/1",
                    "abc/foo/2", "foo/2",
                    "abc/foo/3", "foo/3",
                    "abc/bar/1", "bar/1",
                    "abc/bar/2", "bar/2",
                    "abc/bar/3", "bar/3",
                    "abc/foobar/1", "foobar/1",
                    "abc/foobar/2", "foobar/2",
                    "abc/foobar/3", "foobar/3" }, ids);


                ids.Clear();
                IncludeUtil.GetDocIdFromInclude(reader, "ContactInfoId,,Foo[{0}/abc]", ids);
                Assert.Equal(new[] { "foo/1/abc", "foo/1",
                    "foo/2/abc", "foo/2",
                    "foo/3/abc", "foo/3",
                    "bar/1/abc", "bar/1",
                    "bar/2/abc", "bar/2",
                    "bar/3/abc", "bar/3",
                    "foobar/1/abc", "foobar/1",
                    "foobar/2/abc", "foobar/2",
                    "foobar/3/abc", "foobar/3" }, ids);
            }
        }

        [Fact]
        public void FindDocIdFromPath_with_array_of_with_of_arrays_of_nested_objects_should_work1()
        {
            var obj = new DynamicJsonValue
            {
                ["Name"] = "John Doe",
                ["ContactInfoId"] = new DynamicJsonArray(new[]
                {
                    GetNestedObjectArray("foo"),
                    GetNestedObjectArray("bar"),
                    GetNestedObjectArray("foobar")
                })
            };

            using (var context = JsonOperationContext.ShortTermSingleUse())
            using (var reader = context.ReadObject(obj, "foo"))
            {
                var ids = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                IncludeUtil.GetDocIdFromInclude(reader, "ContactInfoId,,Bar.Foo", ids);
                Assert.Equal(new[] { "foo/1", "foo/2", "foo/3", "bar/1", "bar/2", "bar/3", "foobar/1", "foobar/2", "foobar/3" }, ids);

                ids.Clear();
                IncludeUtil.GetDocIdFromInclude(reader, "ContactInfoId,,Bar.Foo(abc/)", ids);
                Assert.Equal(new[] { "abc/foo/1", "foo/1",
                    "abc/foo/2", "foo/2",
                    "abc/foo/3", "foo/3",
                    "abc/bar/1", "bar/1",
                    "abc/bar/2", "bar/2",
                    "abc/bar/3", "bar/3",
                    "abc/foobar/1", "foobar/1",
                    "abc/foobar/2", "foobar/2",
                    "abc/foobar/3", "foobar/3" }, ids);

                ids.Clear();
                IncludeUtil.GetDocIdFromInclude(reader, "ContactInfoId,,Bar.Foo[{0}/abc]", ids);
                Assert.Equal(new[] { "foo/1/abc", "foo/1",
                    "foo/2/abc", "foo/2",
                    "foo/3/abc", "foo/3",
                    "bar/1/abc", "bar/1",
                    "bar/2/abc", "bar/2",
                    "bar/3/abc", "bar/3",
                    "foobar/1/abc", "foobar/1",
                    "foobar/2/abc", "foobar/2",
                    "foobar/3/abc", "foobar/3" }, ids);
            }
        }

        [Fact]
        public void FindDocIdFromPath_with_array_of_with_of_arrays_of_nested_objects_should_work2()
        {
            var obj = new DynamicJsonValue
            {
                ["Name"] = "John Doe",
                ["ContactInfoId"] = new DynamicJsonArray(new[]
                {
                    new DynamicJsonValue
                    {
                        ["Foo"] = GetNestedObjectArray("foo")
                    },
                    new DynamicJsonValue
                    {
                        ["Foo"] = GetNestedObjectArray("bar")
                    },
                    new DynamicJsonValue
                    {
                        ["Foo"] = GetNestedObjectArray("foobar")
                    },
                })
            };

            using (var context = JsonOperationContext.ShortTermSingleUse())
            using (var reader = context.ReadObject(obj, "foo"))
            {
                var ids = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                IncludeUtil.GetDocIdFromInclude(reader, "ContactInfoId,Foo,Bar.Foo", ids);
                Assert.Equal(new[] { "foo/1", "foo/2", "foo/3", "bar/1", "bar/2", "bar/3", "foobar/1", "foobar/2", "foobar/3" }, ids);

                ids.Clear();
                IncludeUtil.GetDocIdFromInclude(reader, "ContactInfoId,Foo,Bar.Foo(abc/)", ids);
                Assert.Equal(new[] { "abc/foo/1", "foo/1",
                    "abc/foo/2", "foo/2",
                    "abc/foo/3", "foo/3",
                    "abc/bar/1", "bar/1",
                    "abc/bar/2", "bar/2",
                    "abc/bar/3", "bar/3",
                    "abc/foobar/1", "foobar/1",
                    "abc/foobar/2", "foobar/2",
                    "abc/foobar/3", "foobar/3" }, ids);

                ids.Clear();
                IncludeUtil.GetDocIdFromInclude(reader, "ContactInfoId,Foo,Bar.Foo[{0}/abc]", ids);
                Assert.Equal(new[] { "foo/1/abc", "foo/1",
                    "foo/2/abc", "foo/2",
                    "foo/3/abc", "foo/3",
                    "bar/1/abc", "bar/1",
                    "bar/2/abc", "bar/2",
                    "bar/3/abc", "bar/3",
                    "foobar/1/abc", "foobar/1",
                    "foobar/2/abc", "foobar/2",
                    "foobar/3/abc", "foobar/3" }, ids);
            }
        }

        [Fact]
        public void FindDocIdFromPath_with_array_of_with_of_arrays_of_nested_objects_should_work3()
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
                            ["Foo"] = GetNestedObjectArray("foo")
                        }
                    },
                    new DynamicJsonValue
                    {
                        ["BarX"] = new DynamicJsonValue
                        {
                            ["Foo"] = GetNestedObjectArray("bar")
                        }
                    },
                    new DynamicJsonValue
                    {
                        ["BarX"] = new DynamicJsonValue
                        {
                            ["Foo"] = GetNestedObjectArray("foobar")
                        }
                    },
                })
            };

            using (var context = JsonOperationContext.ShortTermSingleUse())
            using (var reader = context.ReadObject(obj, "foo"))
            {
                var ids = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                IncludeUtil.GetDocIdFromInclude(reader, "ContactInfoId,BarX.Foo,Bar.Foo", ids);
                Assert.Equal(new[] { "foo/1", "foo/2", "foo/3", "bar/1", "bar/2", "bar/3", "foobar/1", "foobar/2", "foobar/3" }, ids);

                ids.Clear();
                IncludeUtil.GetDocIdFromInclude(reader, "ContactInfoId,BarX.Foo,Bar.Foo(abc/)", ids);
                Assert.Equal(new[] { "abc/foo/1", "foo/1",
                    "abc/foo/2", "foo/2",
                    "abc/foo/3", "foo/3",
                    "abc/bar/1", "bar/1",
                    "abc/bar/2", "bar/2",
                    "abc/bar/3", "bar/3",
                    "abc/foobar/1", "foobar/1",
                    "abc/foobar/2", "foobar/2",
                    "abc/foobar/3", "foobar/3" }, ids);

                ids.Clear();
                IncludeUtil.GetDocIdFromInclude(reader, "ContactInfoId,BarX.Foo,Bar.Foo[{0}/abc]", ids);
                Assert.Equal(new[] { "foo/1/abc", "foo/1",
                    "foo/2/abc", "foo/2",
                    "foo/3/abc", "foo/3",
                    "bar/1/abc", "bar/1",
                    "bar/2/abc", "bar/2",
                    "bar/3/abc", "bar/3",
                    "foobar/1/abc", "foobar/1",
                    "foobar/2/abc", "foobar/2",
                    "foobar/3/abc", "foobar/3" }, ids);
            }
        }

        private static DynamicJsonArray GetStringArray(string prefix)
        {
            return new DynamicJsonArray(new[] { $"{prefix}/1", $"{prefix}/2", $"{prefix}/3" });
        }

        private static DynamicJsonArray GetObjectArray(string prefix)
        {
            return new DynamicJsonArray(new[]
            {
                new DynamicJsonValue
                {
                    ["Foo"] = $"{prefix}/1"
                },
                new DynamicJsonValue
                {
                    ["Foo"] = $"{prefix}/2"
                },
                new DynamicJsonValue
                {
                    ["Foo"] = $"{prefix}/3"
                },
            });
        }

        private static DynamicJsonArray GetNestedObjectArray(string prefix)
        {
            return new DynamicJsonArray(new[]
            {
                new DynamicJsonValue
                {
                    ["Bar"] = new DynamicJsonValue
                    {
                        ["Foo"] = $"{prefix}/1"
                    }
                },
                new DynamicJsonValue
                {
                    ["Bar"] = new DynamicJsonValue
                    {
                        ["Foo"] = $"{prefix}/2"
                    }
                },
                new DynamicJsonValue
                {
                    ["Bar"] = new DynamicJsonValue
                    {
                        ["Foo"] = $"{prefix}/3"
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
                IncludeUtil.GetDocIdFromInclude(reader, "Foo.Bar.C,A.X.Y", ids);
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
                IncludeUtil.GetDocIdFromInclude(reader, "Foo.Bar.C,A.X.Y(ccc/)", ids);
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
                IncludeUtil.GetDocIdFromInclude(reader, "Foo.Bar.C,A.X.Y[{0}/ccc]", ids);
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
                IncludeUtil.GetDocIdFromInclude(reader, "ContactInfoId,(foo/)", ids);
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
                IncludeUtil.GetDocIdFromInclude(reader, "ContactInfoId,[{0}/foo]", ids);
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
                IncludeUtil.GetDocIdFromInclude(reader, "ContactInfoId,Foo(foo/)", ids);
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
                IncludeUtil.GetDocIdFromInclude(reader, "ContactInfoId,Foo[{0}/foo]", ids);
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
                IncludeUtil.GetDocIdFromInclude(reader, "ContactInfoId,", ids);
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
                IncludeUtil.GetDocIdFromInclude(reader, "Foo.Bar.ContactInfoId,", ids);
                Assert.Equal(new[] { "foo/1", "foo/2", "foo/3" }, ids);

                ids.Clear();
                IncludeUtil.GetDocIdFromInclude(reader, "Foo.ContactInfoId2,", ids);
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
                IncludeUtil.GetDocIdFromInclude(reader, "ContactInfoId1,", ids);
                Assert.Equal(new object[] { "1", "2", "3" }, ids);

                ids.Clear();
                IncludeUtil.GetDocIdFromInclude(reader, "ContactInfoId2,", ids);
                Assert.Equal(new object[] { "1.1", "2.2", "3.3" }, ids);

                ids.Clear();
                IncludeUtil.GetDocIdFromInclude(reader, "ContactInfoId3,", ids);
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
                IncludeUtil.GetDocIdFromInclude(reader, "ExtendedInfo.ContactInfoId", ids);
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
                IncludeUtil.GetDocIdFromInclude(reader, "ExtendedInfo.ContactInfoId(contacts/)", ids);
                Assert.Equal("contacts/1", ids.FirstOrDefault());

                //edge cases
                ids.Clear();
                IncludeUtil.GetDocIdFromInclude(reader, "ExtendedInfo.ContactInfoId()", ids);
                Assert.Equal("1", ids.FirstOrDefault());

                ids.Clear();
                IncludeUtil.GetDocIdFromInclude(reader, "ExtendedInfo.ContactInfoId(c/)", ids);
                Assert.Equal("c/1", ids.FirstOrDefault());

                ids.Clear();
                IncludeUtil.GetDocIdFromInclude(reader, "ExtendedInfo.ContactInfoId(ca/)", ids);
                Assert.Equal("ca/1", ids.FirstOrDefault());

                ids.Clear();
                IncludeUtil.GetDocIdFromInclude(reader, "ExtendedInfo.ContactInfoId(/)", ids);
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
                IncludeUtil.GetDocIdFromInclude(reader, "ExtendedInfo.ContactInfoId[{0}/contacts]", ids);
                Assert.Equal("1/contacts", ids.FirstOrDefault());

                //edge cases
                ids.Clear();
                IncludeUtil.GetDocIdFromInclude(reader, "ExtendedInfo.ContactInfoId[]", ids);
                Assert.Equal("1", ids.FirstOrDefault());

                ids.Clear();
                IncludeUtil.GetDocIdFromInclude(reader, "ExtendedInfo.ContactInfoId[{0}/c]", ids);
                Assert.Equal("1/c", ids.FirstOrDefault());

                ids.Clear();
                IncludeUtil.GetDocIdFromInclude(reader, "ExtendedInfo.ContactInfoId[{0}/ca]", ids);
                Assert.Equal("1/ca", ids.FirstOrDefault());

                ids.Clear();
                IncludeUtil.GetDocIdFromInclude(reader, "ExtendedInfo.ContactInfoId[{0}/]", ids);
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
                IncludeUtil.GetDocIdFromInclude(reader, "ExtendedInfo.ContactInfoId(contacts/)", ids);
                Assert.Equal("contacts/megadevice", ids.FirstOrDefault());

                //edge cases
                ids.Clear();
                IncludeUtil.GetDocIdFromInclude(reader, "ExtendedInfo.ContactInfoId()", ids);
                Assert.Equal("megadevice", ids.FirstOrDefault());

                ids.Clear();
                IncludeUtil.GetDocIdFromInclude(reader, "ExtendedInfo.ContactInfoId(c/)", ids);
                Assert.Equal("c/megadevice", ids.FirstOrDefault());

                ids.Clear();
                IncludeUtil.GetDocIdFromInclude(reader, "ExtendedInfo.ContactInfoId(ca/)", ids);
                Assert.Equal("ca/megadevice", ids.FirstOrDefault());

                ids.Clear();
                IncludeUtil.GetDocIdFromInclude(reader, "ExtendedInfo.ContactInfoId(/)", ids);
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
                IncludeUtil.GetDocIdFromInclude(reader, "ExtendedInfo.ContactInfoId[{0}/contacts]", ids);
                Assert.Equal("megadevice/contacts", ids.FirstOrDefault());

                //edge cases
                ids.Clear();
                IncludeUtil.GetDocIdFromInclude(reader, "ExtendedInfo.ContactInfoId[]", ids);
                Assert.Equal("megadevice", ids.FirstOrDefault());

                ids.Clear();
                IncludeUtil.GetDocIdFromInclude(reader, "ExtendedInfo.ContactInfoId[{0}/c]", ids);
                Assert.Equal("megadevice/c", ids.FirstOrDefault());

                ids.Clear();
                IncludeUtil.GetDocIdFromInclude(reader, "ExtendedInfo.ContactInfoId[{0}/ca]", ids);
                Assert.Equal("megadevice/ca", ids.FirstOrDefault());

                ids.Clear();
                IncludeUtil.GetDocIdFromInclude(reader, "ExtendedInfo.ContactInfoId[{0}/]", ids);
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
                IncludeUtil.GetDocIdFromInclude(reader, "ExtendedInfo1.ExtendedInfo2.ExtendedInfo3.ContactInfoId1", ids);
                Assert.Equal("contacts/1", ids.FirstOrDefault());

                ids.Clear();
                IncludeUtil.GetDocIdFromInclude(reader, "ExtendedInfo1.ExtendedInfo2.ExtendedInfo3.ContactInfoId2", ids);
                Assert.Equal("contacts/2", ids.FirstOrDefault());

                ids.Clear();
                IncludeUtil.GetDocIdFromInclude(reader, "ExtendedInfo1.ExtendedInfo2.AdressInfo", ids);
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
                IncludeUtil.GetDocIdFromInclude(reader, "ExtendedInfo.ContactInfoId", ids);

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
                IncludeUtil.GetDocIdFromInclude(reader, "ContactInfoId", ids);
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
                IncludeUtil.GetDocIdFromInclude(reader, "ContactInfoId", ids);
                Assert.Equal("12", ids.FirstOrDefault());

                ids.Clear();
                IncludeUtil.GetDocIdFromInclude(reader, "ContactInfoId2", ids);
                Assert.Equal("56", ids.FirstOrDefault());

                ids.Clear();
                IncludeUtil.GetDocIdFromInclude(reader, "ContactInfoId3", ids);
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

                IncludeUtil.GetDocIdFromInclude(reader, "ContactInfoId(contacts/", ids);
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

                IncludeUtil.GetDocIdFromInclude(reader, "ContactInfoId[{0}/contacts", ids);
                Assert.Empty(ids);

                IncludeUtil.GetDocIdFromInclude(reader, "ContactInfoId[{0/contacts]", ids);
                Assert.Empty(ids);

                IncludeUtil.GetDocIdFromInclude(reader, "ContactInfoId[{}/contacts]", ids);
                Assert.Empty(ids);

                IncludeUtil.GetDocIdFromInclude(reader, "ContactInfoId[0}/contacts]", ids);
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

                IncludeUtil.GetDocIdFromInclude(reader, "ContactInfoId(contacts/)", ids);
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

                IncludeUtil.GetDocIdFromInclude(reader, "ContactInfoId[{0}/contacts]", ids);
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

                IncludeUtil.GetDocIdFromInclude(reader, "ContactInfoId(c/)", ids);
                Assert.Equal("c/1", ids.FirstOrDefault());

                ids.Clear();
                IncludeUtil.GetDocIdFromInclude(reader, "ContactInfoId(ca/)", ids);
                Assert.Equal("ca/1", ids.FirstOrDefault());

                ids.Clear();
                IncludeUtil.GetDocIdFromInclude(reader, "ContactInfoId(caa/)", ids);
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

                IncludeUtil.GetDocIdFromInclude(reader, "ContactInfoId[{0}/c]", ids);
                Assert.Equal("1/c", ids.FirstOrDefault());

                ids.Clear();
                IncludeUtil.GetDocIdFromInclude(reader, "ContactInfoId[{0}/ca]", ids);
                Assert.Equal("1/ca", ids.FirstOrDefault());

                ids.Clear();
                IncludeUtil.GetDocIdFromInclude(reader, "ContactInfoId[{0}/caa]", ids);
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

                IncludeUtil.GetDocIdFromInclude(reader, "AddressInfoId", ids);
                Assert.Equal("addresses/1", ids.FirstOrDefault());

                ids.Clear();
                IncludeUtil.GetDocIdFromInclude(reader, "ContactInfoId", ids);
                Assert.Equal("contacts/1", ids.FirstOrDefault());

                ids.Clear();
                IncludeUtil.GetDocIdFromInclude(reader, "CarInfoId", ids);
                Assert.Equal("cars/1", ids.FirstOrDefault());
            }
        }
    }
}
