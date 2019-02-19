using System.Collections.Generic;
using FastTests;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Threading;
using Xunit;

namespace SlowTests.Utils
{
    public class ConflictResolverTests : NoDisposalNeeded
    {
        [Fact]
        public void CanResolveEmpty()
        {
            using (var ctx = new JsonOperationContext(4096, 16*1024, SharedMultipleUseFlag.None))
            {
                DynamicJsonValue obj1 = new DynamicJsonValue();
                DynamicJsonValue obj2 = new DynamicJsonValue();

                var conflictResovlerAdvisor = new ConflictResolverAdvisor(
                    new List<BlittableJsonReaderObject> {ctx.ReadObject(obj1, "doc/1"), ctx.ReadObject(obj2, "doc/1")},
                    ctx);
                var resolvled = conflictResovlerAdvisor.Resolve().Document;

                Assert.Equal(0, resolvled.Count);
            }
        }

        [Fact]
        public void CanResolveIdentical()
        {
            using (var ctx = new JsonOperationContext(4096, 16*1024, SharedMultipleUseFlag.None))
            {
                DynamicJsonValue obj1 = new DynamicJsonValue();
                DynamicJsonValue obj2 = new DynamicJsonValue();
                obj1["name"] = "Oren";
                obj2["name"] = "Oren";

                var conflictResovlerAdvisor = new ConflictResolverAdvisor(
                    new List<BlittableJsonReaderObject> {ctx.ReadObject(obj1, "doc/1"), ctx.ReadObject(obj2, "doc/1")},
                    ctx);
                var resolvled = conflictResovlerAdvisor.Resolve().Document;

                string name;
                resolvled.TryGet("name", out name);
                Assert.Equal("Oren", name);
            }
        }

        [Fact]
        public void CanResolveTwoEmptyArrays()
        {
            using (var ctx = new JsonOperationContext(4096, 16*1024, SharedMultipleUseFlag.None))
            {
                DynamicJsonValue obj1 = new DynamicJsonValue();
                DynamicJsonValue obj2 = new DynamicJsonValue();
                obj1["name"] = new DynamicJsonArray();
                obj2["name"] = new DynamicJsonArray();

                var conflictResovlerAdvisor = new ConflictResolverAdvisor(
                    new List<BlittableJsonReaderObject> {ctx.ReadObject(obj1, "doc/1"), ctx.ReadObject(obj2, "doc/1")},
                    ctx);
                var resolvled = conflictResovlerAdvisor.Resolve().Document;

                BlittableJsonReaderArray name;
                resolvled.TryGet("name", out name);
                Assert.Equal(0, name.Length);
            }
        }

        [Fact]
        public void CanResolveOneEmptyArraysAndOneWithValue()
        {
            using (var ctx = new JsonOperationContext(4096, 16*1024, SharedMultipleUseFlag.None))
            {
                DynamicJsonValue obj1 = new DynamicJsonValue();
                DynamicJsonValue obj2 = new DynamicJsonValue();
                obj1["name"] = new DynamicJsonArray {1};
                obj2["name"] = new DynamicJsonArray();

                var conflictResovlerAdvisor = new ConflictResolverAdvisor(
                    new List<BlittableJsonReaderObject> {ctx.ReadObject(obj1, "doc/1"), ctx.ReadObject(obj2, "doc/1")},
                    ctx);
                var resolvled = conflictResovlerAdvisor.Resolve().Document;

                BlittableJsonReaderArray name;
                resolvled.TryGet("name", out name);
                Assert.Equal(">>>> auto merged array start", name[0].ToString());
                Assert.Equal((long) 1, name[1]);
                Assert.Equal("<<<< auto merged array end", name[2].ToString());
            }
        }

        [Fact]
        public void CanMergeAdditionalProperties()
        {
            using (var ctx = new JsonOperationContext(4096, 16*1024, SharedMultipleUseFlag.None))
            {
                DynamicJsonValue obj1 = new DynamicJsonValue();
                DynamicJsonValue obj2 = new DynamicJsonValue();
                obj1["Name"] = "Oren";
                obj2["Age"] = 2;

                var conflictResovlerAdvisor = new ConflictResolverAdvisor(
                    new List<BlittableJsonReaderObject> {ctx.ReadObject(obj1, "doc/1"), ctx.ReadObject(obj2, "doc/1")},
                    ctx);
                var resolvled = conflictResovlerAdvisor.Resolve().Document;

                string name;
                int age;

                resolvled.TryGet("Name", out name);
                resolvled.TryGet("Age", out age);
                Assert.Equal(2, age);
                Assert.Equal("Oren", name);
            }
        }

        [Fact]
        public void CanDetectAndSuggestOptionsForConflict_SimpleProp()
        {
            using (var ctx = new JsonOperationContext(4096, 16*1024, SharedMultipleUseFlag.None))
            {
                DynamicJsonValue obj1 = new DynamicJsonValue();
                DynamicJsonValue obj2 = new DynamicJsonValue();
                obj1["Name"] = "Oren";
                obj2["Name"] = "Ayende";

                var conflictResovlerAdvisor = new ConflictResolverAdvisor(
                    new List<BlittableJsonReaderObject> {ctx.ReadObject(obj1, "doc/1"), ctx.ReadObject(obj2, "doc/1")},
                    ctx);
                var resolvled = conflictResovlerAdvisor.Resolve().Document;

                BlittableJsonReaderArray name;
                resolvled.TryGet("Name", out name);
                Assert.Equal(">>>> conflict start", name[0].ToString());
                Assert.Equal("Oren", name[1].ToString());
                Assert.Equal("Ayende", name[2].ToString());
                Assert.Equal("<<<< conflict end", name[3].ToString());
            }
        }

        [Fact]
        public void CanMergeProperties_Nested()
        {
            using (var ctx = new JsonOperationContext(4096, 16*1024, SharedMultipleUseFlag.None))
            {
                DynamicJsonValue obj1 = new DynamicJsonValue();
                DynamicJsonValue obj2 = new DynamicJsonValue();
                obj1["Name"] = new DynamicJsonValue
                {
                    ["First"] = "Oren"
                };
                obj2["Name"] = new DynamicJsonValue
                {
                    ["Last"] = "Eini"
                };

                var conflictResovlerAdvisor = new ConflictResolverAdvisor(
                    new List<BlittableJsonReaderObject> {ctx.ReadObject(obj1, "doc/1"), ctx.ReadObject(obj2, "doc/1")},
                    ctx);
                var resolvled = conflictResovlerAdvisor.Resolve().Document;

                BlittableJsonReaderObject name;
                resolvled.TryGet("Name", out name);
                Assert.Equal("Oren", name["First"].ToString());
                Assert.Equal("Eini", name["Last"].ToString());
            }
        }

        [Fact]
        public void CanDetectConflict_DifferentValues()
        {
            using (var ctx = new JsonOperationContext(4096, 16*1024, SharedMultipleUseFlag.None))
            {
                DynamicJsonValue obj1 = new DynamicJsonValue();
                DynamicJsonValue obj2 = new DynamicJsonValue();
                obj1["Name"] = new DynamicJsonValue
                {
                    ["First"] = "Oren"
                };
                obj2["Name"] = "Eini";

                var conflictResovlerAdvisor = new ConflictResolverAdvisor(
                    new List<BlittableJsonReaderObject> {ctx.ReadObject(obj1, "doc/1"), ctx.ReadObject(obj2, "doc/1")},
                    ctx);
                var resolvled = conflictResovlerAdvisor.Resolve().Document;

                BlittableJsonReaderArray name;
                resolvled.TryGet("Name", out name);
                Assert.Equal(">>>> conflict start", name[0].ToString());
                Assert.Equal("Oren", ((BlittableJsonReaderObject) name[1])["First"].ToString());
                Assert.Equal("Eini", name[2].ToString());
                Assert.Equal("<<<< conflict end", name[3].ToString());
            }
        }

        [Fact]
        public void CanMergeArrays()
        {
            using (var ctx = new JsonOperationContext(4096, 16*1024, SharedMultipleUseFlag.None))
            {
                DynamicJsonValue obj1 = new DynamicJsonValue();
                DynamicJsonValue obj2 = new DynamicJsonValue();
                obj1["Nicks"] = new DynamicJsonArray {"Oren"};
                obj2["Nicks"] = new DynamicJsonArray {"Ayende"};

                var conflictResovlerAdvisor = new ConflictResolverAdvisor(
                    new List<BlittableJsonReaderObject> {ctx.ReadObject(obj1, "doc/1"), ctx.ReadObject(obj2, "doc/1")},
                    ctx);
                var resolvled = conflictResovlerAdvisor.Resolve().Document;

                BlittableJsonReaderArray nicks;
                resolvled.TryGet("Nicks", out nicks);
                Assert.Equal(">>>> auto merged array start", nicks[0].ToString());
                Assert.Equal("Oren", nicks[1].ToString());
                Assert.Equal("Ayende", nicks[2].ToString());
                Assert.Equal("<<<< auto merged array end", nicks[3].ToString());
            }
        }

        [Fact]
        public void CanMergeArrays_SameStart()
        {
            using (var ctx = new JsonOperationContext(4096, 16*1024, SharedMultipleUseFlag.None))
            {
                DynamicJsonValue obj1 = new DynamicJsonValue();
                DynamicJsonValue obj2 = new DynamicJsonValue();
                obj1["Comments"] = new DynamicJsonArray {1, 2, 4};
                obj2["Comments"] = new DynamicJsonArray {1, 2, 5};

                var conflictResovlerAdvisor = new ConflictResolverAdvisor(
                    new List<BlittableJsonReaderObject> {ctx.ReadObject(obj1, "doc/1"), ctx.ReadObject(obj2, "doc/1")},
                    ctx);
                var resolvled = conflictResovlerAdvisor.Resolve().Document;

                BlittableJsonReaderArray comments;
                resolvled.TryGet("Comments", out comments);
                Assert.Equal(">>>> auto merged array start", comments[0].ToString());
                Assert.Equal((long) 1, comments[1]);
                Assert.Equal((long) 2, comments[2]);
                Assert.Equal((long) 4, comments[3]);
                Assert.Equal((long) 5, comments[4]);
                Assert.Equal("<<<< auto merged array end", comments[5].ToString());
            }
        }

        [Fact]
        public void CanResolveEmptyWithMetadata()
        {
            using (var ctx = new JsonOperationContext(4096, 16*1024, SharedMultipleUseFlag.None))
            {
                DynamicJsonValue obj1 = new DynamicJsonValue();
                DynamicJsonValue obj2 = new DynamicJsonValue();
                obj1["@metadata"] = new DynamicJsonValue();
                obj2["@metadata"] = new DynamicJsonValue();

                var conflictResovlerAdvisor = new ConflictResolverAdvisor(
                    new List<BlittableJsonReaderObject> {ctx.ReadObject(obj1, "doc/1"), ctx.ReadObject(obj2, "doc/1")},
                    ctx);
                var resolvled = conflictResovlerAdvisor.Resolve().Metadata;

                Assert.Equal(0, resolvled.Count);
            }
        }

        [Fact]
        public void CanResolveIdenticalMetadata()
        {
            using (var ctx = new JsonOperationContext(4096, 16*1024, SharedMultipleUseFlag.None))
            {
                DynamicJsonValue obj1 = new DynamicJsonValue();
                DynamicJsonValue obj2 = new DynamicJsonValue();
                obj1["@metadata"] = new DynamicJsonValue
                {
                    ["Foo"] = "Bar"
                };
                obj2["@metadata"] = new DynamicJsonValue
                {
                    ["Foo"] = "Bar"
                };

                var conflictResovlerAdvisor = new ConflictResolverAdvisor(
                    new List<BlittableJsonReaderObject> {ctx.ReadObject(obj1, "doc/1"), ctx.ReadObject(obj2, "doc/1")},
                    ctx);
                var resolvled = conflictResovlerAdvisor.Resolve().Metadata;

                string foo;
                resolvled.TryGet("Foo", out foo);
                Assert.Equal("Bar", foo);
            }
        }

        [Fact]
        public void CanResolveTwoEmptyArraysInMetadata()
        {
            using (var ctx = new JsonOperationContext(4096, 16*1024, SharedMultipleUseFlag.None))
            {
                DynamicJsonValue obj1 = new DynamicJsonValue();
                DynamicJsonValue obj2 = new DynamicJsonValue();
                obj1["@metadata"] = new DynamicJsonValue
                {
                    ["Foo"] = new DynamicJsonArray()
                };
                obj2["@metadata"] = new DynamicJsonValue
                {
                    ["Foo"] = new DynamicJsonArray()
                };

                var conflictResovlerAdvisor = new ConflictResolverAdvisor(
                    new List<BlittableJsonReaderObject> {ctx.ReadObject(obj1, "doc/1"), ctx.ReadObject(obj2, "doc/1")},
                    ctx);
                var resolvled = conflictResovlerAdvisor.Resolve().Metadata;

                BlittableJsonReaderArray foo;
                resolvled.TryGet("Foo", out foo);
                Assert.Equal(0, foo.Length);
            }
        }

        [Fact]
        public void CanResolveOneEmptyArraysAndOneWithValueInMetadata()
        {
            using (var ctx = new JsonOperationContext(4096, 16*1024, SharedMultipleUseFlag.None))
            {
                DynamicJsonValue obj1 = new DynamicJsonValue();
                DynamicJsonValue obj2 = new DynamicJsonValue();
                obj1["@metadata"] = new DynamicJsonValue
                {
                    ["Foo"] = new DynamicJsonArray {1}
                };
                obj2["@metadata"] = new DynamicJsonValue
                {
                    ["Foo"] = new DynamicJsonArray()
                };

                var conflictResovlerAdvisor = new ConflictResolverAdvisor(
                    new List<BlittableJsonReaderObject> {ctx.ReadObject(obj1, "doc/1"), ctx.ReadObject(obj2, "doc/1")},
                    ctx);
                var resolvled = conflictResovlerAdvisor.Resolve().Metadata;

                BlittableJsonReaderArray foo;
                resolvled.TryGet("Foo", out foo);
                Assert.Equal(">>>> auto merged array start", foo[0].ToString());
                Assert.Equal((long) 1, foo[1]);
                Assert.Equal("<<<< auto merged array end", foo[2].ToString());
            }
        }


        [Fact]
        public void CanMergeAdditionalMetadata()
        {
            using (var ctx = new JsonOperationContext(4096, 16*1024, SharedMultipleUseFlag.None))
            {
                DynamicJsonValue obj1 = new DynamicJsonValue();
                DynamicJsonValue obj2 = new DynamicJsonValue();
                obj1["@metadata"] = new DynamicJsonValue
                {
                    ["Name"] = "Oren"
                };
                obj2["@metadata"] = new DynamicJsonValue
                {
                    ["Age"] = 2
                };

                var conflictResovlerAdvisor = new ConflictResolverAdvisor(
                    new List<BlittableJsonReaderObject> {ctx.ReadObject(obj1, "doc/1"), ctx.ReadObject(obj2, "doc/1")},
                    ctx);
                var resolvled = conflictResovlerAdvisor.Resolve().Metadata;

                string name;
                int age;
                resolvled.TryGet("Name", out name);
                resolvled.TryGet("Age", out age);
                Assert.Equal("Oren", name);
                Assert.Equal(2, age);
            }
        }

        [Fact]
        public void CanDetectAndSuggestOptionsForConflict_SimpleMetadata()
        {
            using (var ctx = new JsonOperationContext(4096, 16*1024, SharedMultipleUseFlag.None))
            {
                DynamicJsonValue obj1 = new DynamicJsonValue();
                DynamicJsonValue obj2 = new DynamicJsonValue();
                obj1["@metadata"] = new DynamicJsonValue
                {
                    ["Name"] = "Oren"
                };
                obj2["@metadata"] = new DynamicJsonValue
                {
                    ["Name"] = "Ayende"
                };

                var conflictResovlerAdvisor = new ConflictResolverAdvisor(
                    new List<BlittableJsonReaderObject> {ctx.ReadObject(obj1, "doc/1"), ctx.ReadObject(obj2, "doc/1")},
                    ctx);
                var resolvled = conflictResovlerAdvisor.Resolve().Metadata;

                BlittableJsonReaderArray name;
                resolvled.TryGet("Name", out name);
                Assert.Equal(">>>> conflict start", name[0].ToString());
                Assert.Equal("Oren", name[1].ToString());
                Assert.Equal("Ayende", name[2].ToString());
                Assert.Equal("<<<< conflict end", name[3].ToString());
            }
        }
    }
}
