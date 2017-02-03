using System;
using System.IO;
using System.Linq;
using Raven.Server.Config;
using Raven.Server.Config.Settings;
using Raven.Server.Documents;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow;
using Voron.Exceptions;
using Xunit;

namespace FastTests.Server.Documents
{
    public class DocumentsCrud : RavenLowLevelTestBase
    {
        private RavenConfiguration _configuration;
        private DocumentDatabase _documentDatabase;

        public DocumentsCrud()
        {
            _configuration = new RavenConfiguration("foo", ResourceType.Database);
            _configuration.Initialize();

            _configuration.Core.RunInMemory = true;
            _configuration.Core.DataDirectory = new PathSetting(Path.GetTempPath() + @"\crud");

            _documentDatabase = new DocumentDatabase("foo", _configuration, null);
            _documentDatabase.Initialize();
        }

        [Theory]
        [InlineData("users/1")]
        [InlineData("USERs/1")]
        [InlineData("לכובע שלי שלוש פינות")]
        [InlineData("users/111112222233333333333444444445555556")]
        public void PutAndGetDocumentById(string key)
        {
            using (var ctx = DocumentsOperationContext.ShortTermSingleUse(_documentDatabase))
            {
                ctx.OpenWriteTransaction();

                using (var doc = ctx.ReadObject(new DynamicJsonValue
                {
                    ["Name"] = key
                }, key, BlittableJsonDocumentBuilder.UsageMode.ToDisk))
                {
                    _documentDatabase.DocumentsStorage.Put(ctx, key, null, doc);
                }
                ctx.Transaction.Commit();
            }

            using (var ctx = DocumentsOperationContext.ShortTermSingleUse(_documentDatabase))
            {
                ctx.OpenWriteTransaction();

                var document = _documentDatabase.DocumentsStorage.Get(ctx, key);
                Assert.NotNull(document);
                Assert.Equal(1, document.Etag);
                Assert.Equal(key, document.Key);
                string name;
                document.Data.TryGet("Name", out name);
                Assert.Equal(key, name);

                ctx.Transaction.Commit();
            }
        }

        [Theory]
        [InlineData("users/1")]
        [InlineData("USERs/1")]
        [InlineData("לכובע שלי שלוש פינות")]
        public void CanDelete(string key)
        {
            using (var ctx = DocumentsOperationContext.ShortTermSingleUse(_documentDatabase))
            {
                ctx.OpenWriteTransaction();

                using (var doc = ctx.ReadObject(new DynamicJsonValue
                {
                    ["Name"] = key
                }, key, BlittableJsonDocumentBuilder.UsageMode.ToDisk))
                {
                    _documentDatabase.DocumentsStorage.Put(ctx, key, null, doc);
                }
                ctx.Transaction.Commit();
            }

            using (var ctx = DocumentsOperationContext.ShortTermSingleUse(_documentDatabase))
            {
                ctx.OpenWriteTransaction();

                _documentDatabase.DocumentsStorage.Delete(ctx, key, null);

                ctx.Transaction.Commit();
            }

            using (var ctx = DocumentsOperationContext.ShortTermSingleUse(_documentDatabase))
            {
                ctx.OpenWriteTransaction();

                var document = _documentDatabase.DocumentsStorage.Get(ctx, key);
                Assert.Null(document);

                ctx.Transaction.Commit();
            }
        }

        [Fact]
        public void CanQueryByGlobalEtag()
        {
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
        public void EtagsArePersisted()
        {
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

            Restart();

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
        public void EtagsArePersistedWithDeletes()
        {
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

            Restart();

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

        private void Restart()
        {
            var options = _documentDatabase.DocumentsStorage.Environment.Options;
            options.OwnsPagers = false;
            _documentDatabase.Dispose();
            options.OwnsPagers = true;

            _configuration = new RavenConfiguration("test", ResourceType.Database);
            _configuration.Core.DataDirectory = new PathSetting(Path.GetTempPath() + @"\crud");
            _configuration.Initialize();
            _configuration.Core.RunInMemory = true;

            _documentDatabase = new DocumentDatabase("test", _configuration, null);
            _documentDatabase.Initialize(options);
        }

        [Fact]
        public void CanQueryByPrefix()
        {
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

                var documents = _documentDatabase.DocumentsStorage.GetDocumentsStartingWith(ctx, "users/", null, null, 0, 100).ToList();
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

                    Assert.Throws<ConcurrencyException>(() => _documentDatabase.DocumentsStorage.Put(ctx, "users/1", 1, doc));
                }

                ctx.Transaction.Commit();
            }
        }

        [Fact]
        public void WillVerifyEtags_Existing()
        {
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
                    Assert.Throws<ConcurrencyException>(() => _documentDatabase.DocumentsStorage.Put(ctx, "users/1", 3, doc));
                }

                ctx.Transaction.Commit();
            }
        }

        [Fact]
        public void WillVerifyEtags_OnDeleteExisting()
        {
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
                    Assert.Throws<ConcurrencyException>(() => _documentDatabase.DocumentsStorage.Delete(ctx, "users/1", 3));
                }

                ctx.Transaction.Commit();
            }
        }

        [Fact]
        public void WillVerifyEtags_OnDeleteNotThere()
        {
            using (var ctx = DocumentsOperationContext.ShortTermSingleUse(_documentDatabase))
            {
                ctx.OpenWriteTransaction();

                Assert.Throws<ConcurrencyException>(() => _documentDatabase.DocumentsStorage.Delete(ctx, "users/1", 3));

                ctx.Transaction.Commit();
            }
        }

        [Fact]
        public void WillVerifyEtags_ShouldBeNew()
        {
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
                    Assert.Throws<ConcurrencyException>(() => _documentDatabase.DocumentsStorage.Put(ctx, "users/1", 0, doc));
                }

                ctx.Transaction.Commit();
            }
        }

        [Fact]
        public void PutDocumentWithoutId()
        {
            var key = "users/";
            using (var ctx = DocumentsOperationContext.ShortTermSingleUse(_documentDatabase))
            {
                ctx.OpenWriteTransaction();

                for (int i = 1; i < 5; i++)
                {
                    using (var doc = ctx.ReadObject(new DynamicJsonValue
                    {
                        ["ThisDocId"] = $"{i}"
                    }, key, BlittableJsonDocumentBuilder.UsageMode.ToDisk))
                    {
                        var putResult = _documentDatabase.DocumentsStorage.Put(ctx, key, null, doc);
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
                }, key, BlittableJsonDocumentBuilder.UsageMode.ToDisk))
                {
                    var putResult = _documentDatabase.DocumentsStorage.Put(ctx, key, null, doc);
                    Assert.True(putResult.Etag >= 5);
                    Assert.Equal("users/5", putResult.Key);
                }
                ctx.Transaction.Commit();
            }


        }

        public override void Dispose()
        {
            _documentDatabase.Dispose();

            base.Dispose();
        }
    }
}