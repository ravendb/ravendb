using System;
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
        public async Task FindDocIdFromPath_should_work_in_flat_object()
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
