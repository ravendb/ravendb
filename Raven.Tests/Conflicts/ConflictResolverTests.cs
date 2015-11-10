using System.Collections.Generic;
using Raven.Abstractions.Json;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Conflicts
{
    public class ConflictResolverTests : NoDisposalNeeded
    {
        [Fact]
        public void CanResolveEmpty()
        {
            var conflictsResolver = new ConflictsResolver(new List<RavenJObject> { new RavenJObject(), new RavenJObject() });
            Assert.Equal("{}", conflictsResolver.Resolve().Document);
        }

        [Fact]
        public void CanResolveIdentical()
        {
            var conflictsResolver = new ConflictsResolver(
                                new List<RavenJObject> {
                                new RavenJObject
                                {
                                        {"Name", "Oren"}
                                },
                                new RavenJObject
                                {
                                        {"Name","Oren"}
                                }
                        });
            Assert.Equal(@"{
    ""Name"": ""Oren""
}", conflictsResolver.Resolve().Document);
        }

        [Fact]
        public void CanResolveTwoEmptyArrays()
        {
            var conflictsResolver = new ConflictsResolver(
                                new List<RavenJObject> {
                                        new RavenJObject
                                        {
                                                {"Name", new RavenJArray()}
                                        }, 
                                        new RavenJObject
                                        {
                                                {"Name",new RavenJArray()}
                                        }
                                });
            Assert.Equal(@"{
    ""Name"": []
}", conflictsResolver.Resolve().Document);
        }

        [Fact]
        public void CanResolveOneEmptyArraysAndOneWithValue()
        {
            var conflictsResolver = new ConflictsResolver(
                                new List<RavenJObject> { 
                                        new RavenJObject
                                        {
                                                {"Name", new RavenJArray()}
                                        }, 
                                        new RavenJObject
                                        {
                                                {"Name",new RavenJArray{1}}
                                        }
                                });
            Assert.Equal(@"{
    ""Name"": /*>>>> auto merged array start*/ [
        1
    ]/*<<<< auto merged array end*/
}", conflictsResolver.Resolve().Document);
        }

        [Fact]
        public void CanMergeAdditionalProperties()
        {
            var conflictsResolver = new ConflictsResolver(
                                new List<RavenJObject> { 
                                        new RavenJObject
                                        {
                                                {"Name", "Oren"}
                                        },
                                        new RavenJObject
                                        {
                                                {"Age",2}
                                        }
                                });
            Assert.Equal(@"{
    ""Name"": ""Oren"",
    ""Age"": 2
}", conflictsResolver.Resolve().Document);
        }

        [Fact]
        public void CanDetectAndSuggestOptionsForConflict_SimpleProp()
        {
            var conflictsResolver = new ConflictsResolver(
                                new List<RavenJObject> {
                                        new RavenJObject
                                        {
                                                {"Name", "Oren"}
                                        },
                                        new RavenJObject
                                        {
                                                {"Name", "Ayende"}
                                        }
                                });
            Assert.Equal(@"{
    ""Name"": /*>>>> conflict start*/ [
        ""Oren"",
        ""Ayende""
    ]/*<<<< conflict end*/
}", conflictsResolver.Resolve().Document);
        }

        [Fact]
        public void CanMergeProperties_Nested()
        {
            var conflictsResolver = new ConflictsResolver(
                                new List<RavenJObject> {
                                        new RavenJObject
                                        {
                                                {"Name", new RavenJObject
                                                {
                                                        {"First", "Oren"}
                                                }}
                                        },
                                        new RavenJObject
                                        {
                                                        {"Name", new RavenJObject
                                                        {
                                                                {"Last", "Eini"}	
                                                        }}
                                        }
                                });
            Assert.Equal(@"{
    ""Name"": {
            ""First"": ""Oren"",
            ""Last"": ""Eini""
        }
}", conflictsResolver.Resolve().Document);
        }

        [Fact]
        public void CanDetectConflict_DifferentValues()
        {
            var conflictsResolver = new ConflictsResolver(
                                new List<RavenJObject> {
                                        new RavenJObject
                                        {
                                                {"Name", new RavenJObject
                                                {
                                                        {"First", "Oren"}
                                                }}
                                        },
                                        new RavenJObject
                                        {
                                                        {"Name",	"Eini"}
                                        }
                                });
            Assert.Equal(@"{
    ""Name"": /*>>>> conflict start*/ [
        {
            ""First"": ""Oren""
        },
        ""Eini""
    ]/*<<<< conflict end*/
}", conflictsResolver.Resolve().Document);
        }

        [Fact]
        public void CanDetectAndSuggestOptionsForConflict_NestedProp()
        {
            var conflictsResolver = new ConflictsResolver(
                                new List<RavenJObject> {
                                        new RavenJObject
                                        {
                                                {"Name", "Oren"}
                                        },
                                        new RavenJObject
                                        {
                                                {"Name", "Ayende"}
                                        }
                                });
            Assert.Equal(@"{
    ""Name"": /*>>>> conflict start*/ [
        ""Oren"",
        ""Ayende""
    ]/*<<<< conflict end*/
}", conflictsResolver.Resolve().Document);
        }

        [Fact]
        public void CanMergeArrays()
        {
            var docs = new List<RavenJObject>
            {
                new RavenJObject
                {
                    {"Nicks", new RavenJArray {"Oren"}}
                },
                new RavenJObject
                {
                    {"Nicks", new RavenJArray {"Ayende"}}
                }
            };
            foreach (var doc in docs)
            {
                doc.EnsureCannotBeChangeAndEnableSnapshotting();
            }
            var conflictsResolver = new ConflictsResolver(docs);
            Assert.Equal(@"{
    ""Nicks"": /*>>>> auto merged array start*/ [
        ""Oren"",
        ""Ayende""
    ]/*<<<< auto merged array end*/
}", conflictsResolver.Resolve().Document);
        }

        [Fact]
        public void CanMergeArrays_SameStart()
        {
            var conflictsResolver = new ConflictsResolver(
                                new List<RavenJObject> {
                                        new RavenJObject
                                        {
                                                {"Comments", new RavenJArray{1,2,4}}
                                        },
                                        new RavenJObject
                                        {
                                                {"Comments", new RavenJArray{1,2,5}}
                                        }
                                });
            Assert.Equal(@"{
    ""Comments"": /*>>>> auto merged array start*/ [
        1,
        2,
        4,
        5
    ]/*<<<< auto merged array end*/
}", conflictsResolver.Resolve().Document);
        }

        [Fact]
        public void CanResolveEmptyWithMetadata()
        {
            var conflictsResolver = new ConflictsResolver(new List<RavenJObject>
            {
                new RavenJObject()
                {
                    {
                        "@metadata", new RavenJObject()
                    }
                },
                new RavenJObject()
                {
                    {
                        "@metadata", new RavenJObject()
                    }
                }
            });
            Assert.Equal("{}", conflictsResolver.Resolve().Metadata);
        }

        [Fact]
        public void CanResolveIdenticalMetadata()
        {
            var conflictsResolver = new ConflictsResolver(new List<RavenJObject>
            {
                new RavenJObject()
                {
                    {
                        "@metadata", new RavenJObject()
                        {
                            {
                                "Foo", "Bar"
                            }
                        }
                    }
                },
                new RavenJObject()
                {
                    {
                        "@metadata", new RavenJObject()
                        {
                            {
                                "Foo", "Bar"
                            }
                        }

                    }
                }
            });
            Assert.Equal(@"{
    ""Foo"": ""Bar""
}", conflictsResolver.Resolve().Metadata);
        }

        [Fact]
        public void CanResolveTwoEmptyArraysInMetadata()
        {
            var conflictsResolver = new ConflictsResolver(
                                new List<RavenJObject> {
                                        new RavenJObject
                                        {
                                            {"@metadata", new RavenJObject()
                                            {
                                                {"Bar", new RavenJArray()}
                                            }}
                                        }, 
                                        new RavenJObject
                                        {
                                            {"@metadata", new RavenJObject()
                                            {
                                                {"Bar", new RavenJArray()}
                                            }}
                                        }, 
                                });
            Assert.Equal(@"{
    ""Bar"": []
}", conflictsResolver.Resolve().Metadata);
        }

        [Fact]
        public void CanResolveOneEmptyArraysAndOneWithValueInMetadata()
        {
            var conflictsResolver = new ConflictsResolver(
                                new List<RavenJObject> {
                                        new RavenJObject
                                        {
                                            {"@metadata", new RavenJObject()
                                            {
                                                {"Bar", new RavenJArray(1)}
                                            }}
                                        }, 
                                        new RavenJObject
                                        {
                                            {"@metadata", new RavenJObject()
                                            {
                                                {"Bar", new RavenJArray()}
                                            }}
                                        }, 
                                });
            Assert.Equal(@"{
    ""Bar"": /*>>>> auto merged array start*/ [
        1
    ]/*<<<< auto merged array end*/
}", conflictsResolver.Resolve().Metadata);
        }


        [Fact]
        public void CanMergeAdditionalMetadata()
        {
            var conflictsResolver = new ConflictsResolver(
                                new List<RavenJObject> {
                                        new RavenJObject
                                        {
                                            {"@metadata", new RavenJObject()
                                            {
                                                {"Name", "Oren"}
                                            }}
                                        }, 
                                        new RavenJObject
                                        {
                                            {"@metadata", new RavenJObject()
                                            {
                                                {"Age", 2}
                                            }}
                                        }, 
                                });
            Assert.Equal(@"{
    ""Name"": ""Oren"",
    ""Age"": 2
}", conflictsResolver.Resolve().Metadata);
        }

        [Fact]
        public void CanDetectAndSuggestOptionsForConflict_SimpleMetadata()
        {
            var conflictsResolver = new ConflictsResolver(
                                new List<RavenJObject> {
                                        new RavenJObject
                                        {
                                            {"@metadata", new RavenJObject()
                                            {
                                                {"Name", "Oren"}
                                            }}
                                        }, 
                                        new RavenJObject
                                        {
                                            {"@metadata", new RavenJObject()
                                            {
                                                {"Name", "Ayende"}
                                            }}
                                        }, 
                                });
            Assert.Equal(@"{
    ""Name"": /*>>>> conflict start*/ [
        ""Oren"",
        ""Ayende""
    ]/*<<<< conflict end*/
}", conflictsResolver.Resolve().Metadata);
        }
    }
}
