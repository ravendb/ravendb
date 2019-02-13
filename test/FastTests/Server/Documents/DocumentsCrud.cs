using System;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Raven.Client.Exceptions;
using Raven.Server.Documents;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Xunit;

namespace FastTests.Server.Documents
{
    public class DocumentsCrud : RavenLowLevelTestBase
    {
        private DocumentDatabase _documentDatabase;
        private IDisposable _disposeDatabase;

        public void Initialize([CallerMemberName]string caller = null)
        {
            _disposeDatabase = CreatePersistentDocumentDatabase(NewDataPath(prefix: "DocumentsCrud"), out _documentDatabase, caller);
        }

        [Theory]
        [InlineData("users/1")]
        [InlineData("USERs/1")]
        [InlineData("לכובע שלי שלוש פינות")]
        [InlineData("users/111112222233333333333444444445555556")]
        public void PutAndGetDocumentById(string id)
        {
            Initialize();
            using (var ctx = DocumentsOperationContext.ShortTermSingleUse(_documentDatabase))
            {
                ctx.OpenWriteTransaction();

                using (var doc = ctx.ReadObject(new DynamicJsonValue
                {
                    ["Name"] = id
                }, id, BlittableJsonDocumentBuilder.UsageMode.ToDisk))
                {
                    _documentDatabase.DocumentsStorage.Put(ctx, id, null, doc);
                }
                ctx.Transaction.Commit();
            }

            using (var ctx = DocumentsOperationContext.ShortTermSingleUse(_documentDatabase))
            {
                ctx.OpenWriteTransaction();

                var document = _documentDatabase.DocumentsStorage.Get(ctx, id);
                Assert.NotNull(document);
                Assert.Equal(1, document.Etag);
                Assert.Equal(id, document.Id);
                string name;
                document.Data.TryGet("Name", out name);
                Assert.Equal(id, name);

                ctx.Transaction.Commit();
            }
        }

        [Theory]
        [InlineData("users/1")]
        [InlineData("USERs/1")]
        [InlineData("לכובע שלי שלוש פינות")]
        public void CanDelete(string id)
        {
            Initialize();
            using (var ctx = DocumentsOperationContext.ShortTermSingleUse(_documentDatabase))
            {
                ctx.OpenWriteTransaction();

                using (var doc = ctx.ReadObject(new DynamicJsonValue
                {
                    ["Name"] = id
                }, id, BlittableJsonDocumentBuilder.UsageMode.ToDisk))
                {
                    _documentDatabase.DocumentsStorage.Put(ctx, id, null, doc);
                }
                ctx.Transaction.Commit();
            }

            using (var ctx = DocumentsOperationContext.ShortTermSingleUse(_documentDatabase))
            {
                ctx.OpenWriteTransaction();

                _documentDatabase.DocumentsStorage.Delete(ctx, id, null);

                ctx.Transaction.Commit();
            }

            using (var ctx = DocumentsOperationContext.ShortTermSingleUse(_documentDatabase))
            {
                ctx.OpenWriteTransaction();

                var document = _documentDatabase.DocumentsStorage.Get(ctx, id);
                Assert.Null(document);

                ctx.Transaction.Commit();
            }
        }

        [Fact]
        public void CanQueryByGlobalEtag()
        {
            Initialize();
            using (var ctx = DocumentsOperationContext.ShortTermSingleUse(_documentDatabase))
            {
                ctx.OpenWriteTransaction();

                using (var doc = ctx.ReadObject(new DynamicJsonValue
                {
                    ["Name"] = "Oren",
                    ["@metadata"] = new DynamicJsonValue
                    {
                        ["@collection"] = "Users"
                    }
                }, "users/1", BlittableJsonDocumentBuilder.UsageMode.ToDisk))
                {
                    _documentDatabase.DocumentsStorage.Put(ctx, "users/1", null, doc);
                }
                using (var doc = ctx.ReadObject(new DynamicJsonValue
                {
                    ["Name"] = "Ayende",
                    ["@metadata"] = new DynamicJsonValue
                    {
                        ["@collection"] = "Users"
                    }
                }, "users/2", BlittableJsonDocumentBuilder.UsageMode.ToDisk))
                {
                    _documentDatabase.DocumentsStorage.Put(ctx, "users/2", null, doc);
                }
                using (var doc = ctx.ReadObject(new DynamicJsonValue
                {
                    ["Name"] = "Arava",
                    ["@metadata"] = new DynamicJsonValue
                    {
                        ["@collection"] = "Dogs"
                    }
                }, "pets/1", BlittableJsonDocumentBuilder.UsageMode.ToDisk))
                {
                    _documentDatabase.DocumentsStorage.Put(ctx, "pets/1", null, doc);
                }
                ctx.Transaction.Commit();
            }

            using (var ctx = DocumentsOperationContext.ShortTermSingleUse(_documentDatabase))
            {
                ctx.OpenWriteTransaction();

                var documents = _documentDatabase.DocumentsStorage.GetDocumentsFrom(ctx, 0, 0, 100).ToList();
                Assert.Equal(3, documents.Count);
                string name;
                documents[0].Data.TryGet("Name", out name);
                Assert.Equal("Oren", name);
                documents[1].Data.TryGet("Name", out name);
                Assert.Equal("Ayende", name);
                documents[2].Data.TryGet("Name", out name);
                Assert.Equal("Arava", name);

                ctx.Transaction.Commit();
            }
        }

        [Fact]
        public async Task EtagsArePersisted()
        {
            Initialize();
            using (var ctx = DocumentsOperationContext.ShortTermSingleUse(_documentDatabase))
            {
                ctx.OpenWriteTransaction();

                using (var doc = ctx.ReadObject(new DynamicJsonValue
                {
                    ["Name"] = "Oren",
                    ["@metadata"] = new DynamicJsonValue
                    {
                        ["@collection"] = "Users"
                    }
                }, "users/1", BlittableJsonDocumentBuilder.UsageMode.ToDisk))
                {
                    var putResult = _documentDatabase.DocumentsStorage.Put(ctx, "users/1", null, doc);
                    Assert.Equal(1, putResult.Etag);
                }

                ctx.Transaction.Commit();
            }

            await Restart();

            using (var ctx = DocumentsOperationContext.ShortTermSingleUse(_documentDatabase))
            {
                ctx.OpenWriteTransaction();

                using (var doc = ctx.ReadObject(new DynamicJsonValue
                {
                    ["Name"] = "Oren",
                    ["@metadata"] = new DynamicJsonValue
                    {
                        ["@collection"] = "Users"
                    }
                }, "users/2", BlittableJsonDocumentBuilder.UsageMode.ToDisk))
                {
                    var putResult = _documentDatabase.DocumentsStorage.Put(ctx, "users/2", null, doc);
                    Assert.Equal(2, putResult.Etag);
                }

                ctx.Transaction.Commit();
            }
        }

        [Fact]
        public async Task EtagsArePersistedWithDeletes()
        {
            Initialize();
            using (var ctx = DocumentsOperationContext.ShortTermSingleUse(_documentDatabase))
            {
                ctx.OpenWriteTransaction();

                using (var doc = ctx.ReadObject(new DynamicJsonValue
                {
                    ["Name"] = "Oren",
                    ["@metadata"] = new DynamicJsonValue
                    {
                        ["@collection"] = "Users"
                    }
                }, "users/1", BlittableJsonDocumentBuilder.UsageMode.ToDisk))
                {
                    var putResult = _documentDatabase.DocumentsStorage.Put(ctx, "users/1", null, doc);
                    Assert.Equal(1, putResult.Etag);
                    _documentDatabase.DocumentsStorage.Delete(ctx, "users/1", null);
                }

                ctx.Transaction.Commit();
            }

            await Restart();

            using (var ctx = DocumentsOperationContext.ShortTermSingleUse(_documentDatabase))
            {
                ctx.OpenWriteTransaction();
                using (var doc = ctx.ReadObject(new DynamicJsonValue
                {
                    ["Name"] = "Oren",
                    ["@metadata"] = new DynamicJsonValue
                    {
                        ["@collection"] = "Users"
                    }
                }, "users/2", BlittableJsonDocumentBuilder.UsageMode.ToDisk))
                {
                    var putResult = _documentDatabase.DocumentsStorage.Put(ctx, "users/2", null, doc);

                    //this should be 3 because in the use-case of this test,
                    //the tombstone that was created when users/1 was deleted, will have etag == 2
                    //thus, next document that is created will have etag == 3
                    Assert.Equal(3, putResult.Etag);
                }

                ctx.Transaction.Commit();
            }
        }

        private async Task Restart()
        {
            _documentDatabase.Dispose();

            Server.ServerStore.DatabasesLandlord.UnloadDirectly(_documentDatabase.Name);

            _documentDatabase = await GetDatabase(_documentDatabase.Name);
        }

        [Fact]
        public void CanQueryByPrefix()
        {
            Initialize();
            using (var ctx = DocumentsOperationContext.ShortTermSingleUse(_documentDatabase))
            {
                ctx.OpenWriteTransaction();

                using (var doc = ctx.ReadObject(new DynamicJsonValue
                {
                    ["Name"] = "Oren",
                    ["@metadata"] = new DynamicJsonValue
                    {
                        ["@collection"] = "Users"
                    }
                }, "users/10", BlittableJsonDocumentBuilder.UsageMode.ToDisk))
                {
                    _documentDatabase.DocumentsStorage.Put(ctx, "users/10", null, doc);
                }
                using (var doc = ctx.ReadObject(new DynamicJsonValue
                {
                    ["Name"] = "Ayende",
                    ["@metadata"] = new DynamicJsonValue
                    {
                        ["@collection"] = "Users"
                    }
                }, "users/02", BlittableJsonDocumentBuilder.UsageMode.ToDisk))
                {
                    _documentDatabase.DocumentsStorage.Put(ctx, "users/02", null, doc);
                }
                using (var doc = ctx.ReadObject(new DynamicJsonValue
                {
                    ["Name"] = "Arava",
                    ["@metadata"] = new DynamicJsonValue
                    {
                        ["@collection"] = "Dogs"
                    }
                }, "pets/1", BlittableJsonDocumentBuilder.UsageMode.ToDisk))
                {
                    _documentDatabase.DocumentsStorage.Put(ctx, "pets/1", null, doc);
                }
                ctx.Transaction.Commit();
            }

            using (var ctx = DocumentsOperationContext.ShortTermSingleUse(_documentDatabase))
            {
                ctx.OpenWriteTransaction();

                var documents = _documentDatabase.DocumentsStorage.GetDocumentsStartingWith(ctx, "users/", null, null, null, 0, 100).ToList();
                Assert.Equal(2, documents.Count);
                string name;

                documents[0].Data.TryGet("Name", out name);
                Assert.Equal("Ayende", name);
                documents[1].Data.TryGet("Name", out name);
                Assert.Equal("Oren", name);
                ctx.Transaction.Commit();
            }
        }

        [Fact]
        public void CanQueryByCollectionEtag()
        {
            Initialize();
            using (var ctx = DocumentsOperationContext.ShortTermSingleUse(_documentDatabase))
            {
                ctx.OpenWriteTransaction();

                using (var doc = ctx.ReadObject(new DynamicJsonValue
                {
                    ["Name"] = "Oren",
                    ["@metadata"] = new DynamicJsonValue
                    {
                        ["@collection"] = "Users"
                    }
                }, "users/1", BlittableJsonDocumentBuilder.UsageMode.ToDisk))
                {
                    _documentDatabase.DocumentsStorage.Put(ctx, "users/1", null, doc);
                }

                using (var doc = ctx.ReadObject(new DynamicJsonValue
                {
                    ["Name"] = "Arava",
                    ["@metadata"] = new DynamicJsonValue
                    {
                        ["@collection"] = "Dogs"
                    }
                }, "pets/1", BlittableJsonDocumentBuilder.UsageMode.ToDisk))
                {
                    _documentDatabase.DocumentsStorage.Put(ctx, "pets/1", null, doc);
                }
                using (var doc = ctx.ReadObject(new DynamicJsonValue
                {
                    ["Name"] = "Ayende",
                    ["@metadata"] = new DynamicJsonValue
                    {
                        ["@collection"] = "Users"
                    }
                }, "users/2", BlittableJsonDocumentBuilder.UsageMode.ToDisk))
                {
                    _documentDatabase.DocumentsStorage.Put(ctx, "users/2", null, doc);
                }
                ctx.Transaction.Commit();
            }

            using (var ctx = DocumentsOperationContext.ShortTermSingleUse(_documentDatabase))
            {
                ctx.OpenWriteTransaction();

                var documents = _documentDatabase.DocumentsStorage.GetDocumentsFrom(ctx, "Users", 0, 0, 10).ToList();
                Assert.Equal(2, documents.Count);
                string name;
                documents[0].Data.TryGet("Name", out name);
                Assert.Equal("Oren", name);
                documents[1].Data.TryGet("Name", out name);
                Assert.Equal("Ayende", name);

                ctx.Transaction.Commit();
            }
        }

        [Fact]
        public void WillVerifyEtags_New()
        {
            Initialize();
            using (var ctx = DocumentsOperationContext.ShortTermSingleUse(_documentDatabase))
            {
                ctx.OpenWriteTransaction();

                using (var doc = ctx.ReadObject(new DynamicJsonValue
                {
                    ["Name"] = "Oren",
                    ["@metadata"] = new DynamicJsonValue
                    {
                        ["@collection"] = "Users"
                    }
                }, "users/1", BlittableJsonDocumentBuilder.UsageMode.ToDisk))
                {
                    var changeVector = ctx.GetLazyString($"A:1-{_documentDatabase.DocumentsStorage.Environment.Base64Id}");
                    Assert.Throws<ConcurrencyException>(() => _documentDatabase.DocumentsStorage.Put(ctx, "users/1", changeVector, doc));
                }

                ctx.Transaction.Commit();
            }
        }

        [Fact]
        public void WillVerifyEtags_Existing()
        {
            Initialize();
            using (var ctx = DocumentsOperationContext.ShortTermSingleUse(_documentDatabase))
            {
                ctx.OpenWriteTransaction();

                using (var doc = ctx.ReadObject(new DynamicJsonValue
                {
                    ["Name"] = "Oren",
                    ["@metadata"] = new DynamicJsonValue
                    {
                        ["@collection"] = "Users"
                    }
                }, "users/1", BlittableJsonDocumentBuilder.UsageMode.ToDisk))
                {
                    _documentDatabase.DocumentsStorage.Put(ctx, "users/1", null, doc);
                    var changeVector = ctx.GetLazyString($"A:3-{_documentDatabase.DocumentsStorage.Environment.Base64Id}");
                    Assert.Throws<ConcurrencyException>(() => _documentDatabase.DocumentsStorage.Put(ctx, "users/1", changeVector, doc));
                }

                ctx.Transaction.Commit();
            }
        }

        [Fact]
        public void WillVerifyEtags_OnDeleteExisting()
        {
            Initialize();
            using (var ctx = DocumentsOperationContext.ShortTermSingleUse(_documentDatabase))
            {
                ctx.OpenWriteTransaction();

                using (var doc = ctx.ReadObject(new DynamicJsonValue
                {
                    ["Name"] = "Oren",
                    ["@metadata"] = new DynamicJsonValue
                    {
                        ["@collection"] = "Users"
                    }
                }, "users/1", BlittableJsonDocumentBuilder.UsageMode.ToDisk))
                {
                    _documentDatabase.DocumentsStorage.Put(ctx, "users/1", null, doc);
                    var changeVector = ctx.GetLazyString($"A:3-{_documentDatabase.DocumentsStorage.Environment.Base64Id}");
                    Assert.Throws<ConcurrencyException>(() => _documentDatabase.DocumentsStorage.Delete(ctx, "users/1", changeVector));
                }

                ctx.Transaction.Commit();
            }
        }

        [Fact]
        public void WillVerifyEtags_OnDeleteNotThere()
        {
            Initialize();
            using (var ctx = DocumentsOperationContext.ShortTermSingleUse(_documentDatabase))
            {
                ctx.OpenWriteTransaction();
                var changeVector = ctx.GetLazyString($"A:3-{_documentDatabase.DocumentsStorage.Environment.Base64Id}");
                Assert.Throws<ConcurrencyException>(() => _documentDatabase.DocumentsStorage.Delete(ctx, "users/1", changeVector));

                ctx.Transaction.Commit();
            }
        }

        [Fact]
        public void WillVerifyEtags_ShouldBeNew()
        {
            Initialize();
            using (var ctx = DocumentsOperationContext.ShortTermSingleUse(_documentDatabase))
            {
                ctx.OpenWriteTransaction();

                using (var doc = ctx.ReadObject(new DynamicJsonValue
                {
                    ["Name"] = "Oren",
                    ["@metadata"] = new DynamicJsonValue
                    {
                        ["@collection"] = "Users"
                    }
                }, "users/1", BlittableJsonDocumentBuilder.UsageMode.ToDisk))
                {
                    _documentDatabase.DocumentsStorage.Put(ctx, "users/1", null, doc);
                    Assert.Throws<ConcurrencyException>(() => _documentDatabase.DocumentsStorage.Put(ctx, "users/1", "A:1-abc", doc));
                }

                ctx.Transaction.Commit();
            }
        }

        [Fact]
        public void WillVerifyEtags_VerifyNew()
        {
            Initialize();
            using (var ctx = DocumentsOperationContext.ShortTermSingleUse(_documentDatabase))
            {
                ctx.OpenWriteTransaction();

                using (var doc = ctx.ReadObject(new DynamicJsonValue
                {
                    ["Name"] = "Oren",
                    ["@metadata"] = new DynamicJsonValue
                    {
                        ["@collection"] = "Users"
                    }
                }, "users/1", BlittableJsonDocumentBuilder.UsageMode.ToDisk))
                {
                    var changeVector = ctx.GetLazyString("");
                    _documentDatabase.DocumentsStorage.Put(ctx, "users/1", changeVector, doc);
                }

                ctx.Transaction.Commit();
            }
        }


        [Fact]
        public void PutDocumentWithoutId()
        {
            Initialize();
            var id = "users/";
            using (var ctx = DocumentsOperationContext.ShortTermSingleUse(_documentDatabase))
            {
                ctx.OpenWriteTransaction();

                for (int i = 1; i < 5; i++)
                {
                    using (var doc = ctx.ReadObject(new DynamicJsonValue
                    {
                        ["ThisDocId"] = $"{i}"
                    }, id + i, BlittableJsonDocumentBuilder.UsageMode.ToDisk))
                    {
                        var putResult = _documentDatabase.DocumentsStorage.Put(ctx, id + i, null, doc);
                        Assert.Equal(i, putResult.Etag);
                    }
                }
                ctx.Transaction.Commit();
            }

            using (var ctx = DocumentsOperationContext.ShortTermSingleUse(_documentDatabase))
            {
                ctx.OpenWriteTransaction();

                _documentDatabase.DocumentsStorage.Delete(ctx, "users/2", null);

                ctx.Transaction.Commit();
            }

            using (var ctx = DocumentsOperationContext.ShortTermSingleUse(_documentDatabase))
            {
                ctx.OpenWriteTransaction();

                using (var doc = ctx.ReadObject(new DynamicJsonValue
                {
                    ["ThisDocId"] = "2"
                }, id, BlittableJsonDocumentBuilder.UsageMode.ToDisk))
                {
                    var putResult = _documentDatabase.DocumentsStorage.Put(ctx, id + 5, null, doc);
                    Assert.True(putResult.Etag >= 5);
                    Assert.Equal("users/5", putResult.Id);
                }
                ctx.Transaction.Commit();
            }
        }

        public override void Dispose()
        {
            _disposeDatabase.Dispose();
            base.Dispose();
        }
    }
}
