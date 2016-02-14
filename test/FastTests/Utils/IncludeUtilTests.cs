using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Server.Json;
using Raven.Server.Json.Parsing;
using Raven.Server.Utils;
using Xunit;

namespace FastTests.Utils
{
    public class IncludeUtilTests
    {
        [Fact]
        public async Task FindDocIdFromPath_with_array_of_nested_objects_should_work1()
        {
            var obj = new DynamicJsonValue
            {
                ["Name"] = "John Doe",
                ["Foo"] = new DynamicJsonValue
                {
                    ["Bar"] = new DynamicJsonValue
                    {
                        ["C"] = new DynamicJsonArray
                        {
                            Items = new Queue<object>(new []
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
                    }
                },
                ["ContactInfoId"] = new DynamicJsonArray
                {
                    Items = new Queue<object>(new[]
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
                }
            };

            using (var pool = new UnmanagedBuffersPool("test"))
            using (var context = new RavenOperationContext(pool))
            using (var reader = await context.ReadObject(obj, "foo"))
            {
                var ids = IncludeUtil.GetDocIdFromInclude(reader, "ContactInfoId,Foo").ToList();
                Assert.Equal(new[] { "foobar/1", "foobar/2", "foobar/3", "foobar/4" }, ids);

                ids = IncludeUtil.GetDocIdFromInclude(reader, "Foo.Bar.C,X.Y").ToList();
                Assert.Equal(new[] { "ccc/1", "ccc/2", "ccc/3", "ccc/5" }, ids);
            }
        }

        [Fact]
        public async Task FindDocIdFromPath_with_array_of_nested_objects_should_work2()
        {
            var obj = new DynamicJsonValue
            {
                ["Name"] = "John Doe",
                ["Foo"] = new DynamicJsonValue
                {
                    ["Bar"] = new DynamicJsonValue
                    {
                        ["C"] = new DynamicJsonArray
                        {
                            Items = new Queue<object>(new[]
                            {
                                new DynamicJsonValue
                                {
                                    ["A"] = new DynamicJsonValue {
                                        ["X"] = new DynamicJsonValue
                                        {
                                            ["Y"] = "ccc/1"
                                        }
                                    }
                                },
                                new DynamicJsonValue
                                {
                                    ["A"] = new DynamicJsonValue {
                                        ["X"] = new DynamicJsonValue
                                        {
                                            ["YABC"] = "ccc/2"
                                        }
                                    }
                                },
                                new DynamicJsonValue
                                {
                                    ["A"] = new DynamicJsonValue {
                                        ["X"] = new DynamicJsonValue
                                        {
                                            ["Y"] = "ccc/3"
                                        }
                                    }
                                },
                                new DynamicJsonValue
                                {
                                    ["A"] = new DynamicJsonValue {
                                        ["X"] = new DynamicJsonValue
                                        {
                                            ["Y"] = "ccc/4"
                                        }
                                    }
                                },
                                new DynamicJsonValue
                                {
                                    ["A"] = new DynamicJsonValue {
                                        ["X"] = new DynamicJsonValue
                                        {
                                            ["Y"] = "ccc/5"
                                        }
                                    }
                                },
                            })
                        }
                    }
                }
            };
            using (var pool = new UnmanagedBuffersPool("test"))
            using (var context = new RavenOperationContext(pool))
            using (var reader = await context.ReadObject(obj, "foo"))
            {
                var ids = IncludeUtil.GetDocIdFromInclude(reader, "Foo.Bar.C,A.X.Y").ToList();
                Assert.Equal(new[] { "ccc/1", "ccc/3", "ccc/4", "ccc/5" }, ids);
            }
        }


        [Fact]
        public async Task FindDocIdFromPath_with_array_of_nested_objects_with_prefix_should_work()
        {
            var obj = new DynamicJsonValue
            {
                ["Name"] = "John Doe",
                ["Foo"] = new DynamicJsonValue
                {
                    ["Bar"] = new DynamicJsonValue
                    {
                        ["C"] = new DynamicJsonArray
                        {
                            Items = new Queue<object>(new[]
                            {
                                new DynamicJsonValue
                                {
                                    ["A"] = new DynamicJsonValue {
                                        ["X"] = new DynamicJsonValue
                                        {
                                            ["Y"] = 1
                                        }
                                    }
                                },
                                new DynamicJsonValue
                                {
                                    ["A"] = new DynamicJsonValue {
                                        ["X"] = new DynamicJsonValue
                                        {
                                            ["YABC"] = 2
                                        }
                                    }
                                },
                                new DynamicJsonValue
                                {
                                    ["A"] = new DynamicJsonValue {
                                        ["X"] = new DynamicJsonValue
                                        {
                                            ["Y"] = 3
                                        }
                                    }
                                },
                                new DynamicJsonValue
                                {
                                    ["A"] = new DynamicJsonValue {
                                        ["X"] = new DynamicJsonValue
                                        {
                                            ["Y"] = 4
                                        }
                                    }
                                },
                                new DynamicJsonValue
                                {
                                    ["A"] = new DynamicJsonValue {
                                        ["X"] = new DynamicJsonValue
                                        {
                                            ["Y"] = 5
                                        }
                                    }
                                },
                            })
                        }
                    }
                }
            };
            using (var pool = new UnmanagedBuffersPool("test"))
            using (var context = new RavenOperationContext(pool))
            using (var reader = await context.ReadObject(obj, "foo"))
            {
                var ids = IncludeUtil.GetDocIdFromInclude(reader, "Foo.Bar.C,A.X.Y(ccc/)").ToList();
                Assert.Equal(new[] { "ccc/1", "ccc/3", "ccc/4", "ccc/5" }, ids);
            }
        }

        [Fact]
        public async Task FindDocIdFromPath_with_array_selection_should_work_in_flat_object_with_prefix()
        {
            var obj = new DynamicJsonValue
            {
                ["Name"] = "John Doe",
                ["ContactInfoId"] = new DynamicJsonArray
                {
                    Items = new Queue<object>(new object[] { 1, 2, 3 })
                }
            };
            using (var pool = new UnmanagedBuffersPool("test"))
            using (var context = new RavenOperationContext(pool))
            using (var reader = await context.ReadObject(obj, "foo"))
            {
                var ids = IncludeUtil.GetDocIdFromInclude(reader, "ContactInfoId,(foo/)").ToList();
                Assert.Equal(new[] { "foo/1", "foo/2", "foo/3" }, ids);
            }
        }

        [Fact]
        public async Task FindDocIdFromPath_with_array_selection_should_work_in_flat_object1()
        {
            var obj = new DynamicJsonValue
            {
                ["Name"] = "John Doe",
                ["ContactInfoId"] = new DynamicJsonArray
                {
                    Items = new Queue<object>(new[] { "foo/1", "foo/2", "foo/3" })
                }
            };
            using (var pool = new UnmanagedBuffersPool("test"))
            using (var context = new RavenOperationContext(pool))
            using (var reader = await context.ReadObject(obj, "foo"))
            {
                var ids = IncludeUtil.GetDocIdFromInclude(reader, "ContactInfoId,").ToList();
                Assert.Equal(new[] { "foo/1", "foo/2", "foo/3" },ids);
            }
        }

        [Fact]
        public async Task FindDocIdFromPath_with_array_selection_should_work_in_nested_object2()
        {
            var obj = new DynamicJsonValue
            {
                ["Name"] = "John Doe",
                ["Foo"] = new DynamicJsonValue
                {
                    ["Bar"] = new DynamicJsonValue
                    {
                        ["ContactInfoId"] = new DynamicJsonArray
                        {
                            Items = new Queue<object>(new[] { "foo/1", "foo/2", "foo/3" })
                        }
                    },
                    ["ContactInfoId2"] = new DynamicJsonArray
                    {
                        Items = new Queue<object>(new[] { "foo/1", "foo/2", "foo/3" })
                    }
                }
            };
            using (var pool = new UnmanagedBuffersPool("test"))
            using (var context = new RavenOperationContext(pool))
            using (var reader = await context.ReadObject(obj, "foo"))
            {
                var ids = IncludeUtil.GetDocIdFromInclude(reader, "Foo.Bar.ContactInfoId,").ToList();
                Assert.Equal(new[] { "foo/1", "foo/2", "foo/3" }, ids);

                ids = IncludeUtil.GetDocIdFromInclude(reader, "Foo.ContactInfoId2,").ToList();
                Assert.Equal(new[] { "foo/1", "foo/2", "foo/3" }, ids);
            }
        }

        [Fact]
        public async Task FindDocIdFromPath_with_array_selection_should_work_in_flat_object2()
        {
            var obj = new DynamicJsonValue
            {
                ["Name"] = "John Doe",
                ["ContactInfoId1"] = new DynamicJsonArray
                {
                    Items = new Queue<object>(new object[] { 1, 2, 3 })
                },
                ["ContactInfoId2"] = new DynamicJsonArray
                {
                    Items = new Queue<object>(new object[] { 1.1, 2.2, 3.3 })
                },
                ["ContactInfoId3"] = new DynamicJsonArray
                {
                    Items = new Queue<object>(new object[] { (long)1, (long)2, (long)3 })
                }
            };
            using (var pool = new UnmanagedBuffersPool("test"))
            using (var context = new RavenOperationContext(pool))
            using (var reader = await context.ReadObject(obj, "foo"))
            {
                var ids = IncludeUtil.GetDocIdFromInclude(reader, "ContactInfoId1,").ToList();
                Assert.Equal(new object[] { "1", "2", "3" }, ids);

                ids = IncludeUtil.GetDocIdFromInclude(reader, "ContactInfoId2,").ToList();
                Assert.Equal(new object[] { "1.1", "2.2", "3.3" }, ids);

                ids = IncludeUtil.GetDocIdFromInclude(reader, "ContactInfoId3,").ToList();
                Assert.Equal(new object[] { "1", "2", "3" }, ids);
            }
        }

        [Fact]
        public async Task FindDocIdFromPath_should_return_value_for_single_level_nested_path()
        {
            var obj = new DynamicJsonValue
            {
                ["Name"] = "John Doe",
                ["ExtendedInfo"] = new DynamicJsonValue
                {
                    ["ContactInfoId"] = "contacts/1"
                }
            };
            using (var pool = new UnmanagedBuffersPool("test"))
            using (var context = new RavenOperationContext(pool))
            using (var reader = await context.ReadObject(obj, "foo"))
            {
                var id = IncludeUtil.GetDocIdFromInclude(reader, "ExtendedInfo.ContactInfoId").First();				
                Assert.Equal("contacts/1", id);

            }
        }

        [Fact]
        public async Task FindDocIdFromPath_should_return_value_for_single_level_nested_path_with_prefix()
        {
            var obj = new DynamicJsonValue
            {
                ["Name"] = "John Doe",
                ["ExtendedInfo"] = new DynamicJsonValue
                {
                    ["ContactInfoId"] = 1
                }
            };
            using (var pool = new UnmanagedBuffersPool("test"))
            using (var context = new RavenOperationContext(pool))
            using (var reader = await context.ReadObject(obj, "foo"))
            {
                var id = IncludeUtil.GetDocIdFromInclude(reader, "ExtendedInfo.ContactInfoId(contacts/)").FirstOrDefault();
                Assert.NotNull(id);
                Assert.Equal("contacts/1", id);

                //edge cases
                id = IncludeUtil.GetDocIdFromInclude(reader, "ExtendedInfo.ContactInfoId()").FirstOrDefault();
                Assert.Equal("1", id);
                id = IncludeUtil.GetDocIdFromInclude(reader, "ExtendedInfo.ContactInfoId(c/)").FirstOrDefault();
                Assert.Equal("c/1", id);
                id = IncludeUtil.GetDocIdFromInclude(reader, "ExtendedInfo.ContactInfoId(ca/)").FirstOrDefault();
                Assert.Equal("ca/1", id);
                id = IncludeUtil.GetDocIdFromInclude(reader, "ExtendedInfo.ContactInfoId(/)").FirstOrDefault();
                Assert.Equal("/1", id);
            }
        }

        [Fact]
        public async Task FindDocIdFromPath_should_return_value_for_single_level_nested_path_with_prefix_and_string_value()
        {
            var obj = new DynamicJsonValue
            {
                ["Name"] = "John Doe",
                ["ExtendedInfo"] = new DynamicJsonValue
                {
                    ["ContactInfoId"] = "megadevice"
                }
            };
            using (var pool = new UnmanagedBuffersPool("test"))
            using (var context = new RavenOperationContext(pool))
            using (var reader = await context.ReadObject(obj, "foo"))
            {
                var id = IncludeUtil.GetDocIdFromInclude(reader, "ExtendedInfo.ContactInfoId(contacts/)").FirstOrDefault();
                Assert.NotNull(id);
                Assert.Equal("contacts/megadevice", id);

                //edge cases
                id = IncludeUtil.GetDocIdFromInclude(reader, "ExtendedInfo.ContactInfoId()").FirstOrDefault();
                Assert.Equal("megadevice", id);
                id = IncludeUtil.GetDocIdFromInclude(reader, "ExtendedInfo.ContactInfoId(c/)").FirstOrDefault();
                Assert.Equal("c/megadevice", id);
                id = IncludeUtil.GetDocIdFromInclude(reader, "ExtendedInfo.ContactInfoId(ca/)").FirstOrDefault();
                Assert.Equal("ca/megadevice", id);
                id = IncludeUtil.GetDocIdFromInclude(reader, "ExtendedInfo.ContactInfoId(/)").FirstOrDefault();
                Assert.Equal("/megadevice", id);
            }
        }

        [Fact]
        public async Task FindDocIdFromPath_should_return_value_for_multiple_level_nested_path()
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
            using (var pool = new UnmanagedBuffersPool("test"))
            using (var context = new RavenOperationContext(pool))
            using (var reader = await context.ReadObject(obj, "foo"))
            {
                var id = IncludeUtil.GetDocIdFromInclude(reader, "ExtendedInfo1.ExtendedInfo2.ExtendedInfo3.ContactInfoId1").First();
                Assert.Equal("contacts/1", id);
                id = IncludeUtil.GetDocIdFromInclude(reader, "ExtendedInfo1.ExtendedInfo2.ExtendedInfo3.ContactInfoId2").First();
                Assert.Equal("contacts/2", id);
                id = IncludeUtil.GetDocIdFromInclude(reader, "ExtendedInfo1.ExtendedInfo2.AdressInfo").First();
                Assert.Equal("address/1", id);
            }
        }


        [Fact]
        public async Task FindDocIdFromPath_should_return_empty_for_incorrect_path()
        {
            var obj = new DynamicJsonValue
            {
                ["Name"] = "John Doe",
                ["ContactInfoId"] = "contacts/1"
            };
            using (var pool = new UnmanagedBuffersPool("test"))
            using (var context = new RavenOperationContext(pool))
            using (var reader = await context.ReadObject(obj, "foo"))
            {
                Assert.Empty(IncludeUtil.GetDocIdFromInclude(reader, "ExtendedInfo.ContactInfoId"));
            }
        }

        [Fact]
        public async Task FindDocIdFromPath_should_work_in_flat_object1()
        {
            var obj = new DynamicJsonValue
            {
                ["Name"] = "John Doe",
                ["ContactInfoId"] = "contacts/1"
            };
            using (var pool = new UnmanagedBuffersPool("test"))
            using (var context = new RavenOperationContext(pool))
            using (var reader = await context.ReadObject(obj, "foo"))
            {
                var id = IncludeUtil.GetDocIdFromInclude(reader, "ContactInfoId").First();
                Assert.Equal("contacts/1", id);
            }
        }

        [Fact]
        public async Task FindDocIdFromPath_should_work_in_flat_object2()
        {
            var obj = new DynamicJsonValue
            {
                ["Name"] = "John Doe",
                //for numbers, only int32, int64 and double are supported
                ["ContactInfoId"] = 12,
                ["ContactInfoId2"] = (long)56,
                ["ContactInfoId3"] = 78.89, //this one is double
            };
            using (var pool = new UnmanagedBuffersPool("test"))
            using (var context = new RavenOperationContext(pool))
            using (var reader = await context.ReadObject(obj, "foo"))
            {
                var id = IncludeUtil.GetDocIdFromInclude(reader, "ContactInfoId").FirstOrDefault();
                Assert.Equal("12", id);
                id = IncludeUtil.GetDocIdFromInclude(reader, "ContactInfoId2").FirstOrDefault();
                Assert.Equal("56", id);
                id = IncludeUtil.GetDocIdFromInclude(reader, "ContactInfoId3").FirstOrDefault();
                Assert.Equal("78.89", id);				
            }
        }

        [Fact]
        public async Task FindDocIdFromPath_should_return_empty_with_incomplete_prefix_in_flat_object()
        {
            var obj = new DynamicJsonValue
            {
                ["Name"] = "John Doe",
                ["ContactInfoId"] = 1
            };
            using (var pool = new UnmanagedBuffersPool("test"))
            using (var context = new RavenOperationContext(pool))
            using (var reader = await context.ReadObject(obj, "foo"))
            {
                Assert.Empty(IncludeUtil.GetDocIdFromInclude(reader, "ContactInfoId(contacts/").ToList());
            }
        }

        [Fact]
        public async Task FindDocIdFromPath_should_work_with_prefix_in_flat_object()
        {
            var obj = new DynamicJsonValue
            {
                ["Name"] = "John Doe",
                ["ContactInfoId"] = 1
            };
            using (var pool = new UnmanagedBuffersPool("test"))
            using (var context = new RavenOperationContext(pool))
            using (var reader = await context.ReadObject(obj, "foo"))
            {
                var id = IncludeUtil.GetDocIdFromInclude(reader, "ContactInfoId(contacts/)").First();
                Assert.Equal("contacts/1", id);
            }
        }

        [Fact]
        public async Task FindDocIdFromPath_should_work_with_very_short_prefix_in_flat_object()
        {
            var obj = new DynamicJsonValue
            {
                ["Name"] = "John Doe",
                ["ContactInfoId"] = 1
            };
            using (var pool = new UnmanagedBuffersPool("test"))
            using (var context = new RavenOperationContext(pool))
            using (var reader = await context.ReadObject(obj, "foo"))
            {
                var id = IncludeUtil.GetDocIdFromInclude(reader, "ContactInfoId(c/)").First();
                Assert.Equal("c/1", id);
                id = IncludeUtil.GetDocIdFromInclude(reader, "ContactInfoId(ca/)").First();
                Assert.Equal("ca/1", id);
                id = IncludeUtil.GetDocIdFromInclude(reader, "ContactInfoId(caa/)").First();
                Assert.Equal("caa/1", id);
            }
        }

        [Fact]
        public async Task FindDocIdFromPath_with_multiple_targets_should_work_in_flat_object()
        {
            var obj = new DynamicJsonValue
            {
                ["Name"] = "John Doe",
                ["ContactInfoId"] = "contacts/1",
                ["AddressInfoId"] = "addresses/1",
                ["CarInfoId"] = "cars/1"
            };
            using (var pool = new UnmanagedBuffersPool("test"))
            using (var context = new RavenOperationContext(pool))
            using (var reader = await context.ReadObject(obj, "foo"))
            {
                var id = IncludeUtil.GetDocIdFromInclude(reader, "AddressInfoId").First();
                Assert.Equal("addresses/1", id);
                id = IncludeUtil.GetDocIdFromInclude(reader, "ContactInfoId").First();
                Assert.Equal("contacts/1", id);
                id = IncludeUtil.GetDocIdFromInclude(reader, "CarInfoId").First();
                Assert.Equal("cars/1", id);
            }
        }
    }
}
