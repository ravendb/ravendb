using System;
using System.IO;
using System.Linq;
using Raven.Client.Connection;
using Raven.Client.Document;
using Raven.Server.Config;
using Raven.Server.Documents;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils.Metrics;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Voron.Exceptions;
using Xunit;

namespace FastTests.Server.Documents
{
    public class DocumentsCrud : IDisposable
    {
        private RavenConfiguration _configuration;
        private DocumentDatabase _documentDatabase;
        private readonly UnmanagedBuffersPool _unmanagedBuffersPool;

        public DocumentsCrud()
        {
            _configuration = new RavenConfiguration();
            _configuration.Initialize();

            _configuration.Core.RunInMemory = true;
            _configuration.Core.DataDirectory = Path.GetTempPath() + @"\crud";

            _documentDatabase = new DocumentDatabase("foo", _configuration, new MetricsScheduler(), new LoggerSetup(Path.GetTempPath(), LogMode.None));
            _documentDatabase.Initialize(new HttpJsonRequestFactory(1),new DocumentConvention());

            _unmanagedBuffersPool = new UnmanagedBuffersPool("test");
        }

        [Theory]
        [InlineData("users/1")]
        [InlineData("USERs/1")]
        [InlineData("לכובע שלי שלוש פינות")]
        [InlineData("users/111112222233333333333444444445555556")]
        public void PutAndGetDocumentById(string key)
        {
            using (var ctx = new DocumentsOperationContext(_unmanagedBuffersPool, _documentDatabase))
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

            using (var ctx = new DocumentsOperationContext(_unmanagedBuffersPool, _documentDatabase))
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
            using (var ctx = new DocumentsOperationContext(_unmanagedBuffersPool, _documentDatabase))
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

            using (var ctx = new DocumentsOperationContext(_unmanagedBuffersPool, _documentDatabase))
            {
                ctx.OpenWriteTransaction();

                _documentDatabase.DocumentsStorage.Delete(ctx, key, null);

                ctx.Transaction.Commit();
            }

            using (var ctx = new DocumentsOperationContext(_unmanagedBuffersPool, _documentDatabase))
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
            using (var ctx = new DocumentsOperationContext(_unmanagedBuffersPool, _documentDatabase))
            {
                ctx.OpenWriteTransaction();

                using (var doc = ctx.ReadObject(new DynamicJsonValue
                {
                    ["Name"] = "Oren",
                    ["@metadata"] = new DynamicJsonValue
                    {
                        ["Raven-Entity-Name"] = "Users"
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
                        ["Raven-Entity-Name"] = "Users"
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
                        ["Raven-Entity-Name"] = "Dogs"
                    }
                }, "pets/1", BlittableJsonDocumentBuilder.UsageMode.ToDisk))
                {
                    _documentDatabase.DocumentsStorage.Put(ctx, "pets/1", null, doc);
                }
                ctx.Transaction.Commit();
            }

            using (var ctx = new DocumentsOperationContext(_unmanagedBuffersPool, _documentDatabase))
            {
                ctx.OpenWriteTransaction();

                var documents = _documentDatabase.DocumentsStorage.GetDocumentsAfter(ctx, 0, 0, 100).ToList();
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
            using (var ctx = new DocumentsOperationContext(_unmanagedBuffersPool, _documentDatabase))
            {
                ctx.OpenWriteTransaction();

                using (var doc = ctx.ReadObject(new DynamicJsonValue
                {
                    ["Name"] = "Oren",
                    ["@metadata"] = new DynamicJsonValue
                    {
                        ["Raven-Entity-Name"] = "Users"
                    }
                }, "users/1", BlittableJsonDocumentBuilder.UsageMode.ToDisk))
                {
                    var putResult = _documentDatabase.DocumentsStorage.Put(ctx, "users/1", null, doc);
                    Assert.Equal(1, putResult.ETag);
                }

                ctx.Transaction.Commit();
            }

            Restart();

            using (var ctx = new DocumentsOperationContext(_unmanagedBuffersPool, _documentDatabase))
            {
                ctx.OpenWriteTransaction();

                using (var doc = ctx.ReadObject(new DynamicJsonValue
                {
                    ["Name"] = "Oren",
                    ["@metadata"] = new DynamicJsonValue
                    {
                        ["Raven-Entity-Name"] = "Users"
                    }
                }, "users/2", BlittableJsonDocumentBuilder.UsageMode.ToDisk))
                {
                    var putResult = _documentDatabase.DocumentsStorage.Put(ctx, "users/2", null, doc);
                    Assert.Equal(2, putResult.ETag);
                }

                ctx.Transaction.Commit();
            }
        }

        [Fact]
        public void EtagsArePersistedWithDeletes()
        {
            using (var ctx = new DocumentsOperationContext(_unmanagedBuffersPool, _documentDatabase))
            {
                ctx.OpenWriteTransaction();

                using (var doc = ctx.ReadObject(new DynamicJsonValue
                {
                    ["Name"] = "Oren",
                    ["@metadata"] = new DynamicJsonValue
                    {
                        ["Raven-Entity-Name"] = "Users"
                    }
                }, "users/1", BlittableJsonDocumentBuilder.UsageMode.ToDisk))
                {
                    var putResult = _documentDatabase.DocumentsStorage.Put(ctx, "users/1", null, doc);
                    Assert.Equal(1, putResult.ETag);
                    _documentDatabase.DocumentsStorage.Delete(ctx, "users/1", null);
                }

                ctx.Transaction.Commit();
            }

            Restart();

            using (var ctx = new DocumentsOperationContext(_unmanagedBuffersPool, _documentDatabase))
            {
                ctx.OpenWriteTransaction();
                using (var doc = ctx.ReadObject(new DynamicJsonValue
                {
                    ["Name"] = "Oren",
                    ["@metadata"] = new DynamicJsonValue
                    {
                        ["Raven-Entity-Name"] = "Users"
                    }
                }, "users/2", BlittableJsonDocumentBuilder.UsageMode.ToDisk))
                {
                    var putResult = _documentDatabase.DocumentsStorage.Put(ctx, "users/2", null, doc);
                    Assert.Equal(2, putResult.ETag);
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

            _configuration = new RavenConfiguration();
            _configuration.Core.RunInMemory = true;
            _configuration.Core.DataDirectory = Path.GetTempPath() + @"\crud";
            _configuration.Initialize();

            _documentDatabase = new DocumentDatabase("test", _configuration, new MetricsScheduler(), new LoggerSetup(Path.GetTempPath(),LogMode.None));
            _documentDatabase.Initialize(options, new HttpJsonRequestFactory(16),new DocumentConvention());
        }

        [Fact]
        public void CanQueryByPrefix()
        {
            using (var ctx = new DocumentsOperationContext(_unmanagedBuffersPool, _documentDatabase))
            {
                ctx.OpenWriteTransaction();

                using (var doc = ctx.ReadObject(new DynamicJsonValue
                {
                    ["Name"] = "Oren",
                    ["@metadata"] = new DynamicJsonValue
                    {
                        ["Raven-Entity-Name"] = "Users"
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
                        ["Raven-Entity-Name"] = "Users"
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
                        ["Raven-Entity-Name"] = "Dogs"
                    }
                }, "pets/1", BlittableJsonDocumentBuilder.UsageMode.ToDisk))
                {
                    _documentDatabase.DocumentsStorage.Put(ctx, "pets/1", null, doc);
                }
                ctx.Transaction.Commit();
            }

            using (var ctx = new DocumentsOperationContext(_unmanagedBuffersPool, _documentDatabase))
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
            using (var ctx = new DocumentsOperationContext(_unmanagedBuffersPool, _documentDatabase))
            {
                ctx.OpenWriteTransaction();

                using (var doc = ctx.ReadObject(new DynamicJsonValue
                {
                    ["Name"] = "Oren",
                    ["@metadata"] = new DynamicJsonValue
                    {
                        ["Raven-Entity-Name"] = "Users"
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
                        ["Raven-Entity-Name"] = "Dogs"
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
                        ["Raven-Entity-Name"] = "Users"
                    }
                }, "users/2", BlittableJsonDocumentBuilder.UsageMode.ToDisk))
                {
                    _documentDatabase.DocumentsStorage.Put(ctx, "users/2", null, doc);
                }
                ctx.Transaction.Commit();
            }

            using (var ctx = new DocumentsOperationContext(_unmanagedBuffersPool, _documentDatabase))
            {
                ctx.OpenWriteTransaction();

                var documents = _documentDatabase.DocumentsStorage.GetDocumentsAfter(ctx, "Users", 0, 0, 10).ToList();
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
            using (var ctx = new DocumentsOperationContext(_unmanagedBuffersPool, _documentDatabase))
            {
                ctx.OpenWriteTransaction();

                using (var doc = ctx.ReadObject(new DynamicJsonValue
                {
                    ["Name"] = "Oren",
                    ["@metadata"] = new DynamicJsonValue
                    {
                        ["Raven-Entity-Name"] = "Users"
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
            using (var ctx = new DocumentsOperationContext(_unmanagedBuffersPool, _documentDatabase))
            {
                ctx.OpenWriteTransaction();

                using (var doc = ctx.ReadObject(new DynamicJsonValue
                {
                    ["Name"] = "Oren",
                    ["@metadata"] = new DynamicJsonValue
                    {
                        ["Raven-Entity-Name"] = "Users"
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
            using (var ctx = new DocumentsOperationContext(_unmanagedBuffersPool, _documentDatabase))
            {
                ctx.OpenWriteTransaction();

                using (var doc = ctx.ReadObject(new DynamicJsonValue
                {
                    ["Name"] = "Oren",
                    ["@metadata"] = new DynamicJsonValue
                    {
                        ["Raven-Entity-Name"] = "Users"
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
            using (var ctx = new DocumentsOperationContext(_unmanagedBuffersPool, _documentDatabase))
            {
                ctx.OpenWriteTransaction();

                Assert.Throws<ConcurrencyException>(() => _documentDatabase.DocumentsStorage.Delete(ctx, "users/1", 3));

                ctx.Transaction.Commit();
            }
        }

        [Fact]
        public void WillVerifyEtags_ShouldBeNew()
        {
            using (var ctx = new DocumentsOperationContext(_unmanagedBuffersPool, _documentDatabase))
            {
                ctx.OpenWriteTransaction();

                using (var doc = ctx.ReadObject(new DynamicJsonValue
                {
                    ["Name"] = "Oren",
                    ["@metadata"] = new DynamicJsonValue
                    {
                        ["Raven-Entity-Name"] = "Users"
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
            using (var ctx = new DocumentsOperationContext(_unmanagedBuffersPool, _documentDatabase))
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
                        Assert.Equal(i, putResult.ETag);
                    }
                }
                ctx.Transaction.Commit();
            }

            using (var ctx = new DocumentsOperationContext(_unmanagedBuffersPool, _documentDatabase))
            {
                ctx.OpenWriteTransaction();

                _documentDatabase.DocumentsStorage.Delete(ctx, "users/2", null);

                ctx.Transaction.Commit();
            }

            using (var ctx = new DocumentsOperationContext(_unmanagedBuffersPool, _documentDatabase))
            {
                ctx.OpenWriteTransaction();

                using (var doc = ctx.ReadObject(new DynamicJsonValue
                {
                    ["ThisDocId"] = "2"
                }, key, BlittableJsonDocumentBuilder.UsageMode.ToDisk))
                {
                    var putResult = _documentDatabase.DocumentsStorage.Put(ctx, key, null, doc);
                    Assert.True(putResult.ETag >= 5);
                    Assert.Equal("users/5", putResult.Key);
                }
                ctx.Transaction.Commit();
            }


        }

        public void Dispose()
        {
            _documentDatabase.Dispose();
            _unmanagedBuffersPool.Dispose();
        }
    }
}