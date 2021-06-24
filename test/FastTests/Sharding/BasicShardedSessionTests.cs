using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FastTests.Client;
using Raven.Client;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Documents.Session;
using Raven.Client.Exceptions;
using Raven.Server.Documents;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Json.Parsing;
using Xunit;
using Xunit.Abstractions;
using DocumentsChanges = Raven.Client.Documents.Session.DocumentsChanges;

namespace FastTests.Sharding
{
    public class BasicShardedSessionTests : ShardedTestBase
    {
        public BasicShardedSessionTests(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CRUD_Operations()
        {
            using (var store = GetShardedDocumentStore())
            {
                using (var newSession = store.OpenSession())
                {
                    newSession.Store(new User { Name = "user1" }, "users/1");
                    var user2 = new User {Name = "user2", Age = 1};
                    newSession.Store(user2, "users/2");
                    var user3 = new User { Name = "user3", Age = 1 };
                    newSession.Store(user3, "users/3");
                    newSession.Store(new User { Name = "user4" }, "users/4");
                    
                    newSession.Delete(user2);
                    user3.Age = 3;
                    newSession.SaveChanges();

                    var tempUser = newSession.Load<User>("users/2");
                    Assert.Null(tempUser);
                    tempUser = newSession.Load<User>("users/3");
                    Assert.Equal(tempUser.Age, 3);
                    var user1 = newSession.Load<User>("users/1");
                    var user4 = newSession.Load<User>("users/4");
                    
                    newSession.Delete(user4);
                    user1.Age = 10;
                    newSession.SaveChanges();

                    tempUser = newSession.Load<User>("users/4");
                    Assert.Null(tempUser);
                    tempUser = newSession.Load<User>("users/1");
                    Assert.Equal(tempUser.Age, 10);
                }
            }
        }

        private static IEnumerable<object[]>  GetMetadataStaticFields()
        {
            return typeof(Constants.Documents.Metadata)
                .GetFields( System.Reflection.BindingFlags.Static | System.Reflection.BindingFlags.Public)
                .Select(p => p.GetValue(null).ToString())
                .Distinct()
                .SelectMany(s =>
                {
                    var builder = new StringBuilder(s);
                    //Just replacing one char in the end
                    builder[^1] = builder[^1] == 'a' ? 'b' : 'a';
                    return new[] {new object[] {builder.ToString()}};
                });
        }
        
        [Theory(Skip = "ClusterWide not supported")]
        [MemberData(nameof(GetMetadataStaticFields))]
        public async Task StoreDocument_WheHasUserMetadataPropertyWithLengthEqualsToInternalRavenDbMetadataPropertyLength(string metadataPropNameToTest)
        {
            const string id = "id1";
            const string value = "Value";
            
            using var store = GetShardedDocumentStore();
            using (var session = store.OpenAsyncSession(new SessionOptions{TransactionMode = TransactionMode.ClusterWide}))
            {
                var executor = store.GetRequestExecutor();
                using var dis = executor.ContextPool.AllocateOperationContext(out var context);
                var p = context.ReadObject(new DynamicJsonValue
                {
                    [Constants.Documents.Metadata.Key] = new DynamicJsonValue
                    {
                        [metadataPropNameToTest] = value

                    }
                }, $"{nameof(metadataPropNameToTest)} {metadataPropNameToTest}");
                await session.StoreAsync(p, null, id);
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                var entity = await session.LoadAsync<DynamicJsonValue>(id);
                var metadata = session.Advanced.GetMetadataFor(entity);
                Assert.Equal(value, metadata[metadataPropNameToTest]);
            }
        }

        [Fact]
        public void CRUD_Operations_with_what_changed()
        {
            using (var store = GetShardedDocumentStore())
            {
                using (var newSession = store.OpenSession())
                {
                    newSession.Store(new User { Name = "user1" }, "users/1");
                    var user2 = new User { Name = "user2", Age = 1 };
                    newSession.Store(user2, "users/2");
                    var user3 = new User { Name = "user3", Age = 1 };
                    newSession.Store(user3, "users/3");
                    newSession.Store(new User { Name = "user4" }, "users/4");

                    newSession.Delete(user2);
                    user3.Age = 3;
                    
                    Assert.Equal(newSession.Advanced.WhatChanged().Count, 4);
                    newSession.SaveChanges();

                    var tempUser = newSession.Load<User>("users/2");
                    Assert.Null(tempUser);
                    tempUser = newSession.Load<User>("users/3");
                    Assert.Equal(tempUser.Age, 3);
                    var user1 = newSession.Load<User>("users/1");
                    var user4 = newSession.Load<User>("users/4");

                    newSession.Delete(user4);
                    user1.Age = 10;
                    Assert.Equal(newSession.Advanced.WhatChanged().Count, 2);
                    newSession.SaveChanges();

                    tempUser = newSession.Load<User>("users/4");
                    Assert.Null(tempUser);
                    tempUser = newSession.Load<User>("users/1");
                    Assert.Equal(tempUser.Age, 10);
                }
            }
        }

        [Fact]
        public void CanUseIdentities()
        {
            using (var store = GetShardedDocumentStore())
            {
                using (var s = store.OpenSession())
                {
                    var entity = new User
                    {
                        LastName = "Adi"
                    };

                    s.Store(entity, "users|");
                    s.SaveChanges();
                }

                using (var s = store.OpenSession())
                {
                    var entity = new User
                    {
                        LastName = "Avivi"
                    };

                    s.Store(entity, "users|");
                    s.SaveChanges();
                }

                using (var s = store.OpenSession())
                {
                    var entityWithId1 = s.Load<User>("users/1");
                    var entityWithId2 = s.Load<User>("users/2");

                    Assert.NotNull(entityWithId1);
                    Assert.NotNull(entityWithId2);

                    Assert.Equal("Adi", entityWithId1.LastName);
                    Assert.Equal("Avivi", entityWithId2.LastName);
                }
            }
        }

        [Fact]
        public void CanStoreWithoutId()
        {
            using (var store = GetShardedDocumentStore())
            {
                string id;
                using (var session = store.OpenSession())
                {
                    var user = new User { Name = "Aviv" };
                    session.Store(user);

                    id = user.Id;
                    Assert.NotNull(id);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var loaded = session.Load<User>(id);
                    Assert.Equal("Aviv", loaded.Name);
                }
            }
        }


        [Fact]
        public void CanUseBatchPatchCommand()
        {
            using (var store = GetShardedDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Company
                    {
                        Id = "companies/1",
                        Name = "C1"
                    });

                    session.Store(new Company
                    {
                        Id = "companies/2",
                        Name = "C2"
                    });

                    session.Store(new Company
                    {
                        Id = "companies/3",
                        Name = "C3"
                    });

                    session.Store(new Company
                    {
                        Id = "companies/4",
                        Name = "C4"
                    });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var c1 = session.Load<Company>("companies/1");
                    var c2 = session.Load<Company>("companies/2");
                    var c3 = session.Load<Company>("companies/3");
                    var c4 = session.Load<Company>("companies/4");

                    Assert.Equal("C1", c1.Name);
                    Assert.Equal("C2", c2.Name);
                    Assert.Equal("C3", c3.Name);
                    Assert.Equal("C4", c4.Name);

                    var ids = new List<string>
                    {
                        c1.Id,
                        c3.Id
                    };

                    session.Advanced.Defer(new BatchPatchCommandData(ids, new PatchRequest
                    {
                        Script = "this.Name = 'test';"
                    }, null));

                    session.Advanced.Defer(new BatchPatchCommandData(new List<string> { c4.Id }, new PatchRequest
                    {
                        Script = "this.Name = 'test2';"
                    }, null));

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var c1 = session.Load<Company>("companies/1");
                    var c2 = session.Load<Company>("companies/2");
                    var c3 = session.Load<Company>("companies/3");
                    var c4 = session.Load<Company>("companies/4");

                    Assert.Equal("test", c1.Name);
                    Assert.Equal("C2", c2.Name);
                    Assert.Equal("test", c3.Name);
                    Assert.Equal("test2", c4.Name);
                }

                using (var session = store.OpenSession())
                {
                    var c2 = session.Load<Company>("companies/2");

                    session.Advanced.Defer(new BatchPatchCommandData(new List<(string Id, string ChangeVector)> { (c2.Id, "invalidCV") }, new PatchRequest
                    {
                        Script = "this.Name = 'test2';"
                    }, null));

                    Assert.Throws<ConcurrencyException>(() => session.SaveChanges());
                }

                using (var session = store.OpenSession())
                {
                    var c1 = session.Load<Company>("companies/1");
                    var c2 = session.Load<Company>("companies/2");
                    var c3 = session.Load<Company>("companies/3");
                    var c4 = session.Load<Company>("companies/4");

                    Assert.Equal("test", c1.Name);
                    Assert.Equal("C2", c2.Name);
                    Assert.Equal("test", c3.Name);
                    Assert.Equal("test2", c4.Name);
                }
            }
        }

        [Fact]
        public async Task PutAttachments()
        {
            using (var store = GetShardedDocumentStore())
            {
                var names = new[]
                {
                    "background-photo.jpg",
                    "fileNAME_#$1^%_בעברית.txt",
                    "profile.png",
                };

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User {Name = "Fitzchak"}, "users/1");
                    await session.StoreAsync(new User {Name = "Karmel"}, "users/2");
                    await session.StoreAsync(new User {Name = "Oren"}, "users/3");

                    await using (var profileStream = new MemoryStream(new byte[] {1, 2, 3}))
                    await using (var backgroundStream = new MemoryStream(new byte[] {10, 20, 30, 40, 50}))
                    await using (var fileStream = new MemoryStream(new byte[] {1, 2, 3, 4, 5}))
                    {
                        session.Advanced.Attachments.Store("users/1", names[0], backgroundStream, "ImGgE/jPeG");
                        session.Advanced.Attachments.Store("users/2", names[1], fileStream);
                        session.Advanced.Attachments.Store("users/3", names[2], profileStream, "image/png");
                        await session.SaveChangesAsync();
                    }
                }

                using (var session = store.OpenAsyncSession())
                {
                    for (var i = 0; i < names.Length; i++)
                    {
                        var user = await session.LoadAsync<User>("users/" + (i + 1));
                        var metadata = session.Advanced.GetMetadataFor(user);
                        Assert.Equal(DocumentFlags.HasAttachments.ToString(), metadata[Constants.Documents.Metadata.Flags]);
                        var attachments = metadata.GetObjects(Constants.Documents.Metadata.Attachments);
                        Assert.Equal(1, attachments.Length);
                        var attachment = attachments[0];
                        Assert.Equal(names[i], attachment.GetString(nameof(AttachmentName.Name)));
                        var hash = attachment.GetString(nameof(AttachmentName.Hash));
                        if (i == 0)
                        {
                            Assert.Equal("igkD5aEdkdAsAB/VpYm1uFlfZIP9M2LSUsD6f6RVW9U=", hash);
                            Assert.Equal(5, attachment.GetLong(nameof(AttachmentName.Size)));
                        }
                        else if (i == 1)
                        {
                            Assert.Equal("Arg5SgIJzdjSTeY6LYtQHlyNiTPmvBLHbr/Cypggeco=", hash);
                            Assert.Equal(5, attachment.GetLong(nameof(AttachmentName.Size)));
                        }
                        else if (i == 2)
                        {
                            Assert.Equal("EcDnm3HDl2zNDALRMQ4lFsCO3J2Lb1fM1oDWOk2Octo=", hash);
                            Assert.Equal(3, attachment.GetLong(nameof(AttachmentName.Size)));
                        }
                    }
                }
            }
        }

        [Fact]
        public void CRUD_Operations_With_Array_In_Object()
        {
            using (var store = GetShardedDocumentStore())
            {
                using (var newSession = store.OpenSession())
                {
                    var family = new CRUD.Family()
                    {
                        Names = new[] {"Hibernating Rhinos", "RavenDB"}
                    };
                    newSession.Store(family, "family/1");
                    newSession.SaveChanges();

                    var newFamily = newSession.Load<CRUD.Family>("family/1");

                    newFamily.Names = new[] {"Toli", "Mitzi", "Boki"};
                    Assert.Equal(newSession.Advanced.WhatChanged().Count, 1);
                    newSession.SaveChanges();
                }
            }
        }

        [Fact]
        public void CRUD_Operations_With_Array_In_Object_2()
        {
            using (var store = GetShardedDocumentStore())
            {
                using (var newSession = store.OpenSession())
                {
                    var family = new CRUD.Family()
                    {
                        Names = new[] { "Hibernating Rhinos", "RavenDB" }
                    };
                    newSession.Store(family, "family/1");
                    newSession.SaveChanges();

                    var newFamily = newSession.Load<CRUD.Family>("family/1");
                    newFamily.Names = new[] {"Hibernating Rhinos", "RavenDB"};
                    Assert.Equal(newSession.Advanced.WhatChanged().Count, 0);
                    newFamily.Names = new[] { "RavenDB", "Hibernating Rhinos" };
                    Assert.Equal(newSession.Advanced.WhatChanged().Count, 1);
                    newSession.SaveChanges();
                }
            }
        }

        [Fact]
        public void CRUD_Operations_With_Array_In_Object_3()
        {
            using (var store = GetShardedDocumentStore())
            {
                using (var newSession = store.OpenSession())
                {
                    var family = new CRUD.Family()
                    {
                        Names = new[] { "Hibernating Rhinos", "RavenDB" }
                    };
                    newSession.Store(family, "family/1");
                    newSession.SaveChanges();

                    var newFamily = newSession.Load<CRUD.Family>("family/1");

                    newFamily.Names = new[] { "RavenDB" };
                    Assert.Equal(newSession.Advanced.WhatChanged().Count, 1);
                    newSession.SaveChanges();
                }
            }
        }

        [Fact]
        public void CRUD_Operations_With_Array_In_Object_4()
        {
            using (var store = GetShardedDocumentStore())
            {
                using (var newSession = store.OpenSession())
                {
                    var family = new CRUD.Family()
                    {
                        Names = new[] { "Hibernating Rhinos", "RavenDB" }
                    };
                    newSession.Store(family, "family/1");
                    newSession.SaveChanges();

                    var newFamily = newSession.Load<CRUD.Family>("family/1");

                    newFamily.Names = new[] { "RavenDB", "Hibernating Rhinos", "Toli", "Mitzi", "Boki" };
                    Assert.Equal(newSession.Advanced.WhatChanged().Count, 1);
                    newSession.SaveChanges();
                }
            }
        }

        [Fact]
        public void CRUD_Operations_With_Array_In_Object_6()
        {
            using (var store = GetShardedDocumentStore())
            {
                using (var newSession = store.OpenSession())
                {
                    var family = new CRUD.Family()
                    {
                        Names = new[] { "Hibernating Rhinos", "RavenDB" }
                    };
                    newSession.Store(family, "family/1");
                    newSession.SaveChanges();

                    var newFamily = newSession.Load<CRUD.Family>("family/1");

                    newFamily.Names = new[] { "RavenDB", "Toli" };
                    Assert.Equal(newSession.Advanced.WhatChanged().Count, 1);
                    newSession.SaveChanges();
                }
            }
        }

        [Fact]
        public void CRUD_Operations_With_Null()
        {
            using (var store = GetShardedDocumentStore())
            {
                using (var newSession = store.OpenSession())
                {
                    newSession.Store(new User { Name = null }, "users/1");
                    newSession.SaveChanges();
                    var user = newSession.Load<User>("users/1");
                    Assert.Equal(newSession.Advanced.WhatChanged().Count, 0);
                    user.Age = 3;
                    Assert.Equal(newSession.Advanced.WhatChanged().Count, 1);
                }
            }
        }

        public class Family
        {
            public string[] Names { get; set; }
        }

        public class FamilyMembers
        {
            public CRUD.member[] Members { get; set; }
        }
        public class member
        {
            public string Name { get; set; }
            public int Age { get; set; }
        }

        [Fact]
        public void CRUD_Operations_With_Array_of_objects()
        {
            using (var store = GetShardedDocumentStore())
            {
                using (var newSession = store.OpenSession())
                {
                    var family = new CRUD.FamilyMembers()
                    {
                        Members = new [] {
                            new CRUD.member()
                            {
                                Name = "Hibernating Rhinos",
                                Age = 8
                            },
                            new CRUD.member()
                            {
                                Name = "RavenDB",
                                Age = 4
                            }
                        }
                    };
                    newSession.Store(family, "family/1");
                    newSession.SaveChanges();

                    var newFamily = newSession.Load<CRUD.FamilyMembers>("family/1");
                    newFamily.Members = new[]
                    {
                        new CRUD.member()
                        {
                            Name = "RavenDB",
                            Age = 4
                        },
                        new CRUD.member()
                        {
                            Name = "Hibernating Rhinos",
                            Age = 8
                        }
                    };

                    var changes = newSession.Advanced.WhatChanged();

                    Assert.Equal(1 , changes.Count);
                    Assert.Equal(4 , changes["family/1"].Length);

                    Array.Sort(changes["family/1"], (x, y) => x.FieldFullName.CompareTo(y.FieldFullName));

                    Assert.Equal("Name", changes["family/1"][1].FieldName);
                    Assert.Equal(DocumentsChanges.ChangeType.FieldChanged, changes["family/1"][1].Change);
                    Assert.Equal("Hibernating Rhinos", changes["family/1"][1].FieldOldValue.ToString());
                    Assert.Equal("RavenDB", changes["family/1"][1].FieldNewValue.ToString());

                    Assert.Equal("Age", changes["family/1"][0].FieldName);
                    Assert.Equal(DocumentsChanges.ChangeType.FieldChanged, changes["family/1"][0].Change);
                    Assert.Equal(8L, changes["family/1"][0].FieldOldValue);
                    Assert.Equal(4L, changes["family/1"][0].FieldNewValue);

                    Assert.Equal("Name", changes["family/1"][3].FieldName);
                    Assert.Equal(DocumentsChanges.ChangeType.FieldChanged, changes["family/1"][3].Change);
                    Assert.Equal("RavenDB", changes["family/1"][3].FieldOldValue.ToString());
                    Assert.Equal("Hibernating Rhinos", changes["family/1"][3].FieldNewValue.ToString());

                    Assert.Equal("Age", changes["family/1"][2].FieldName);
                    Assert.Equal(DocumentsChanges.ChangeType.FieldChanged, changes["family/1"][2].Change);
                    Assert.Equal(4L, changes["family/1"][2].FieldOldValue);
                    Assert.Equal(8L, changes["family/1"][2].FieldNewValue);

                    newFamily.Members = new[]
                    {
                        new CRUD.member()
                        {
                            Name = "Toli",
                            Age = 5
                        },
                        new CRUD.member()
                        {
                            Name = "Boki",
                            Age = 15
                        }
                    };

                    changes = newSession.Advanced.WhatChanged();

                    Assert.Equal(1, changes.Count);
                    Assert.Equal(4, changes["family/1"].Length);

                    Array.Sort(changes["family/1"], (x, y) => x.FieldFullName.CompareTo(y.FieldFullName));

                    Assert.Equal("Name", changes["family/1"][1].FieldName);
                    Assert.Equal(DocumentsChanges.ChangeType.FieldChanged, changes["family/1"][1].Change);
                    Assert.Equal("Hibernating Rhinos", changes["family/1"][1].FieldOldValue.ToString());
                    Assert.Equal("Toli", changes["family/1"][1].FieldNewValue.ToString());

                    Assert.Equal("Age", changes["family/1"][0].FieldName);
                    Assert.Equal(DocumentsChanges.ChangeType.FieldChanged, changes["family/1"][0].Change);
                    Assert.Equal(8L, changes["family/1"][0].FieldOldValue);
                    Assert.Equal(5L, changes["family/1"][0].FieldNewValue);

                    Assert.Equal("Name", changes["family/1"][3].FieldName);
                    Assert.Equal(DocumentsChanges.ChangeType.FieldChanged, changes["family/1"][3].Change);
                    Assert.Equal("RavenDB", changes["family/1"][3].FieldOldValue.ToString());
                    Assert.Equal("Boki", changes["family/1"][3].FieldNewValue.ToString());

                    Assert.Equal("Age", changes["family/1"][2].FieldName);
                    Assert.Equal(DocumentsChanges.ChangeType.FieldChanged, changes["family/1"][2].Change);
                    Assert.Equal(4L, changes["family/1"][2].FieldOldValue);
                    Assert.Equal(15L, changes["family/1"][2].FieldNewValue);
                }
            }
        }

        public class Arr1
        {
            public string[] str { get; set; }
        }

        public class Arr2
        {
            public CRUD.Arr1[] arr1 { get; set; }
        }

        [Fact]
        public void CRUD_Operations_With_Array_of_Arrays()
        {
            using (var store = GetShardedDocumentStore())
            {
                using (var newSession = store.OpenSession())
                {
                    var arr = new CRUD.Arr2()
                    {
                        arr1 = new CRUD.Arr1[]
                        {
                            new CRUD.Arr1()
                            {
                                str = new [] {"a", "b"}
                            },
                            new CRUD.Arr1()
                            {
                                str = new [] {"c", "d"}
                            }
                        } 
                    };
                    newSession.Store(arr, "arr/1");
                    newSession.SaveChanges();

                    var newArr = newSession.Load<CRUD.Arr2>("arr/1");
                    newArr.arr1 = new CRUD.Arr1[]
                        {
                            new CRUD.Arr1()
                            {
                                str = new [] {"d", "c"}
                            },
                            new CRUD.Arr1()
                            {
                                str = new [] {"a", "b"}
                            }
                       };
                    var whatChanged = newSession.Advanced.WhatChanged();
                    Assert.Equal(1, whatChanged.Count);

                    var change = whatChanged["arr/1"];
                    Assert.Equal(4, change.Length);
                    Assert.Equal("a", change[0].FieldOldValue.ToString());
                    Assert.Equal("d", change[0].FieldNewValue.ToString());

                    Assert.Equal("b", change[1].FieldOldValue.ToString());
                    Assert.Equal("c", change[1].FieldNewValue.ToString());

                    Assert.Equal("c", change[2].FieldOldValue.ToString());
                    Assert.Equal("a", change[2].FieldNewValue.ToString());

                    Assert.Equal("d", change[3].FieldOldValue.ToString());
                    Assert.Equal("b", change[3].FieldNewValue.ToString());

                    newSession.SaveChanges();
                }

                using (var newSession = store.OpenSession())
                {
                    var newArr = newSession.Load<CRUD.Arr2>("arr/1");
                    newArr.arr1 = new CRUD.Arr1[]
                    {
                        new CRUD.Arr1()
                        {
                            str = new [] {"q", "w"}
                        },
                        new CRUD.Arr1()
                        {
                            str = new [] {"a", "b"}
                        }
                    };
                    var whatChanged = newSession.Advanced.WhatChanged();
                    Assert.Equal(whatChanged.Count, 1);

                    var change = whatChanged["arr/1"];
                    Assert.Equal(2, change.Length);
                    Assert.Equal("d", change[0].FieldOldValue.ToString());
                    Assert.Equal("q", change[0].FieldNewValue.ToString());

                    Assert.Equal("c", change[1].FieldOldValue.ToString());
                    Assert.Equal("w", change[1].FieldNewValue.ToString());
                }
            }
        }

        [Fact]
        public void CRUD_Can_Update_Property_To_Null()
        {
            //RavenDB-8345

            using (var store = GetShardedDocumentStore())
            {
                using (var newSession = store.OpenSession())
                {
                    newSession.Store(new User { Name = "user1" }, "users/1");
                    newSession.SaveChanges();
                }

                using (var newSession = store.OpenSession())
                {
                    var user = newSession.Load<User>("users/1");
                    user.Name = null;
                    newSession.SaveChanges();
                }

                using (var newSession = store.OpenSession())
                {
                    var user = newSession.Load<User>("users/1");
                    Assert.Null(user.Name);
                }
            }
        }

        [Fact]
        public void CRUD_Can_Update_Property_From_Null_To_Object()
        {
            using (var store = GetShardedDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Poc
                    {
                        Name = "aviv",
                        Obj = null
                    }, "pocs/1");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var poc = session.Load<Poc>("pocs/1");
                    Assert.Null(poc.Obj);

                    poc.Obj = new
                    {
                        a = 1,
                        b = "2"
                    };
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var poc = session.Load<Poc>("pocs/1");
                    Assert.NotNull(poc.Obj);
                }
            }
        }

        [Fact(Skip = "Sharded HiLo")]
        public async Task Load_WhenDocumentNotFound_ShouldTrack()
        {
            using var store = GetShardedDocumentStore();
            const string notExistId1 = "notExistId1";
            const string notExistId2 = "notExistId2";
            var user = new User();
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(user);
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                _ = await session.LoadAsync<User>(notExistId1);
            
                Assert.True(session.Advanced.IsLoaded(notExistId1));
                
                _ = await session.LoadAsync<User>(notExistId1);
                Assert.Equal(1, session.Advanced.NumberOfRequests);
            }
            
            using (var session = store.OpenAsyncSession())
            {
                _ = await session.LoadAsync<User>(new []{notExistId1, notExistId2});

                Assert.True(session.Advanced.IsLoaded(notExistId1));
                Assert.True(session.Advanced.IsLoaded(notExistId2));
                
                _ = await session.LoadAsync<User>(new []{notExistId1, notExistId2});
                Assert.Equal(1, session.Advanced.NumberOfRequests);
            }
            
            using (var session = store.OpenAsyncSession())
            {
                _ = await session.LoadAsync<User>(new []{user.Id, notExistId1});

                Assert.True(session.Advanced.IsLoaded(user.Id));
                Assert.True(session.Advanced.IsLoaded(notExistId1));
                
                _ = await session.LoadAsync<User>(notExistId1);
                Assert.Equal(1, session.Advanced.NumberOfRequests);
            }
        }
        
        class Poc
        {
            public string Name { get; set; }
            public object Obj { get; set; }
        }
    }
}
