using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Configuration.Memory;
using Raven.Abstractions.Exceptions;
using Raven.Server.Config;
using Raven.Server.Documents;
using Raven.Server.Json;
using Raven.Server.Json.Parsing;
using Xunit;

namespace FastTests.Server.Documents
{
    public class DocumentsCrud : IDisposable
    {
        private DocumentsStorage _documentsStorage;
        private readonly UnmanagedBuffersPool _unmanagedBuffersPool;

        public DocumentsCrud()
        {
            var configuration = new RavenConfiguration();
            configuration.Core.RunInMemory = true;
            configuration.Core.DataDirectory = Path.GetTempPath() + @"\crud";

            _documentsStorage = new DocumentsStorage("foo", configuration);
            _documentsStorage.Initialize();
            _unmanagedBuffersPool = new UnmanagedBuffersPool("test");
        }

        [Theory]
        [InlineData("users/1")]
        [InlineData("USERs/1")]
        [InlineData("לכובע שלי שלוש פינות")]
        public void PutAndGetDocumentById(string key)
        {
            using (var ctx = new RavenOperationContext(_unmanagedBuffersPool))
            {
                ctx.Transaction = _documentsStorage.Environment.WriteTransaction();

                using (var doc = ctx.ReadObject(new DynamicJsonValue
                {
                    ["Name"] = key
                }, key, BlittableJsonDocumentBuilder.UsageMode.ToDisk))
                {
                    _documentsStorage.Put(ctx, key, null, doc);
                }
                ctx.Transaction.Commit();
            }

            using (var ctx = new RavenOperationContext(_unmanagedBuffersPool))
            {
                ctx.Transaction = _documentsStorage.Environment.WriteTransaction();

                var document = _documentsStorage.Get(ctx, key);
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
            using (var ctx = new RavenOperationContext(_unmanagedBuffersPool))
            {
                ctx.Transaction = _documentsStorage.Environment.WriteTransaction();

                using (var doc =  ctx.ReadObject(new DynamicJsonValue
                {
                    ["Name"] = key
                }, key, BlittableJsonDocumentBuilder.UsageMode.ToDisk))
                {
                    _documentsStorage.Put(ctx, key, null, doc);
                }
                ctx.Transaction.Commit();
            }

            using (var ctx = new RavenOperationContext(_unmanagedBuffersPool))
            {
                ctx.Transaction = _documentsStorage.Environment.WriteTransaction();

                _documentsStorage.Delete(ctx, key, null);

                ctx.Transaction.Commit();
            }

            using (var ctx = new RavenOperationContext(_unmanagedBuffersPool))
            {
                ctx.Transaction = _documentsStorage.Environment.WriteTransaction();

                var document = _documentsStorage.Get(ctx, key);
                Assert.Null(document);

                ctx.Transaction.Commit();
            }
        }

        [Fact]
        public void CanQueryByGlobalEtag()
        {
            using (var ctx = new RavenOperationContext(_unmanagedBuffersPool))
            {
                ctx.Transaction = _documentsStorage.Environment.WriteTransaction();

                using (var doc =  ctx.ReadObject(new DynamicJsonValue
                {
                    ["Name"] = "Oren",
                    ["@metadata"] = new DynamicJsonValue
                    {
                        ["Raven-Entity-Name"] = "Users"
                    }
                }, "users/1", BlittableJsonDocumentBuilder.UsageMode.ToDisk))
                {
                    _documentsStorage.Put(ctx, "users/1", null, doc);
                }
                using (var doc =  ctx.ReadObject(new DynamicJsonValue
                {
                    ["Name"] = "Ayende",
                    ["@metadata"] = new DynamicJsonValue
                    {
                        ["Raven-Entity-Name"] = "Users"
                    }
                }, "users/2", BlittableJsonDocumentBuilder.UsageMode.ToDisk))
                {
                    _documentsStorage.Put(ctx, "users/2", null, doc);
                }
                using (var doc =  ctx.ReadObject(new DynamicJsonValue
                {
                    ["Name"] = "Arava",
                    ["@metadata"] = new DynamicJsonValue
                    {
                        ["Raven-Entity-Name"] = "Dogs"
                    }
                }, "pets/1", BlittableJsonDocumentBuilder.UsageMode.ToDisk))
                {
                    _documentsStorage.Put(ctx, "pets/1", null, doc);
                }
                ctx.Transaction.Commit();
            }

            using (var ctx = new RavenOperationContext(_unmanagedBuffersPool))
            {
                ctx.Transaction = _documentsStorage.Environment.WriteTransaction();

                var documents = _documentsStorage.GetDocumentsAfter(ctx, 0, 0, 100).ToList();
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
            using (var ctx = new RavenOperationContext(_unmanagedBuffersPool))
            {
                ctx.Transaction = _documentsStorage.Environment.WriteTransaction();

                using (var doc =  ctx.ReadObject(new DynamicJsonValue
                {
                    ["Name"] = "Oren",
                    ["@metadata"] = new DynamicJsonValue
                    {
                        ["Raven-Entity-Name"] = "Users"
                    }
                }, "users/1", BlittableJsonDocumentBuilder.UsageMode.ToDisk))
                {
                    var putResult = _documentsStorage.Put(ctx, "users/1", null, doc);
                    Assert.Equal(1, putResult.ETag);
                }

                ctx.Transaction.Commit();
            }

            Restart();

            using (var ctx = new RavenOperationContext(_unmanagedBuffersPool))
            {
                ctx.Transaction = _documentsStorage.Environment.WriteTransaction();

                using (var doc =  ctx.ReadObject(new DynamicJsonValue
                {
                    ["Name"] = "Oren",
                    ["@metadata"] = new DynamicJsonValue
                    {
                        ["Raven-Entity-Name"] = "Users"
                    }
                }, "users/2", BlittableJsonDocumentBuilder.UsageMode.ToDisk))
                {
                    var putResult = _documentsStorage.Put(ctx, "users/2", null, doc);
                    Assert.Equal(2, putResult.ETag);
                }

                ctx.Transaction.Commit();
            }
        }

        [Fact]
        public void EtagsArePersistedWithDeletes()
        {
            using (var ctx = new RavenOperationContext(_unmanagedBuffersPool))
            {
                ctx.Transaction = _documentsStorage.Environment.WriteTransaction();

                using (var doc =  ctx.ReadObject(new DynamicJsonValue
                {
                    ["Name"] = "Oren",
                    ["@metadata"] = new DynamicJsonValue
                    {
                        ["Raven-Entity-Name"] = "Users"
                    }
                }, "users/1", BlittableJsonDocumentBuilder.UsageMode.ToDisk))
                {
                    var putResult = _documentsStorage.Put(ctx, "users/1", null, doc);
                    Assert.Equal(1, putResult.ETag);
                    _documentsStorage.Delete(ctx, "users/1", null);
                }

                ctx.Transaction.Commit();
            }

            Restart();

            using (var ctx = new RavenOperationContext(_unmanagedBuffersPool))
            {
                ctx.Transaction = _documentsStorage.Environment.WriteTransaction();
                using (var doc =  ctx.ReadObject(new DynamicJsonValue
                {
                    ["Name"] = "Oren",
                    ["@metadata"] = new DynamicJsonValue
                    {
                        ["Raven-Entity-Name"] = "Users"
                    }
                }, "users/2", BlittableJsonDocumentBuilder.UsageMode.ToDisk))
                {
                    var putResult = _documentsStorage.Put(ctx, "users/2", null, doc);
                    Assert.Equal(2, putResult.ETag);
                }

                ctx.Transaction.Commit();
            }
        }

        private void Restart()
        {
            var options = _documentsStorage.Environment.Options;
            options.OwnsPagers = false;
            _documentsStorage.Dispose();
            options.OwnsPagers = true;
            var configBuilder = new ConfigurationBuilder()
             .Add(new MemoryConfigurationProvider(new Dictionary<string, string>
             {
                 //["run.in.memory"] = "false",
                 //["system.path"] = Path.GetTempPath() + "\\crud"
             }));
            _documentsStorage = new DocumentsStorage("test", new RavenConfiguration());
            _documentsStorage.Initialize(options);
        }


        [Fact]
        public void CanQueryByPrefix()
        {
            using (var ctx = new RavenOperationContext(_unmanagedBuffersPool))
            {
                ctx.Transaction = _documentsStorage.Environment.WriteTransaction();

                using (var doc =  ctx.ReadObject(new DynamicJsonValue
                {
                    ["Name"] = "Oren",
                    ["@metadata"] = new DynamicJsonValue
                    {
                        ["Raven-Entity-Name"] = "Users"
                    }
                }, "users/10", BlittableJsonDocumentBuilder.UsageMode.ToDisk))
                {
                    _documentsStorage.Put(ctx, "users/10", null, doc);
                }
                using (var doc =  ctx.ReadObject(new DynamicJsonValue
                {
                    ["Name"] = "Ayende",
                    ["@metadata"] = new DynamicJsonValue
                    {
                        ["Raven-Entity-Name"] = "Users"
                    }
                }, "users/02", BlittableJsonDocumentBuilder.UsageMode.ToDisk))
                {
                    _documentsStorage.Put(ctx, "users/02", null, doc);
                }
                using (var doc =  ctx.ReadObject(new DynamicJsonValue
                {
                    ["Name"] = "Arava",
                    ["@metadata"] = new DynamicJsonValue
                    {
                        ["Raven-Entity-Name"] = "Dogs"
                    }
                }, "pets/1", BlittableJsonDocumentBuilder.UsageMode.ToDisk))
                {
                    _documentsStorage.Put(ctx, "pets/1", null, doc);
                }
                ctx.Transaction.Commit();
            }

            using (var ctx = new RavenOperationContext(_unmanagedBuffersPool))
            {
                ctx.Transaction = _documentsStorage.Environment.WriteTransaction();

                var documents = _documentsStorage.GetDocumentsStartingWith(ctx, "users/", null, null, 0, 100).ToList();
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
            using (var ctx = new RavenOperationContext(_unmanagedBuffersPool))
            {
                ctx.Transaction = _documentsStorage.Environment.WriteTransaction();

                using (var doc =  ctx.ReadObject(new DynamicJsonValue
                {
                    ["Name"] = "Oren",
                    ["@metadata"] = new DynamicJsonValue
                    {
                        ["Raven-Entity-Name"] = "Users"
                    }
                }, "users/1", BlittableJsonDocumentBuilder.UsageMode.ToDisk))
                {
                    _documentsStorage.Put(ctx, "users/1", null, doc);
                }

                using (var doc =  ctx.ReadObject(new DynamicJsonValue
                {
                    ["Name"] = "Arava",
                    ["@metadata"] = new DynamicJsonValue
                    {
                        ["Raven-Entity-Name"] = "Dogs"
                    }
                }, "pets/1", BlittableJsonDocumentBuilder.UsageMode.ToDisk))
                {
                    _documentsStorage.Put(ctx, "pets/1", null, doc);
                }
                using (var doc =  ctx.ReadObject(new DynamicJsonValue
                {
                    ["Name"] = "Ayende",
                    ["@metadata"] = new DynamicJsonValue
                    {
                        ["Raven-Entity-Name"] = "Users"
                    }
                }, "users/2", BlittableJsonDocumentBuilder.UsageMode.ToDisk))
                {
                    _documentsStorage.Put(ctx, "users/2", null, doc);
                }
                ctx.Transaction.Commit();
            }

            using (var ctx = new RavenOperationContext(_unmanagedBuffersPool))
            {
                ctx.Transaction = _documentsStorage.Environment.WriteTransaction();

                var documents = _documentsStorage.GetDocumentsAfter(ctx, "Users", 0, 0, 10).ToList();
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
            using (var ctx = new RavenOperationContext(_unmanagedBuffersPool))
            {
                ctx.Transaction = _documentsStorage.Environment.WriteTransaction();

                using (var doc =  ctx.ReadObject(new DynamicJsonValue
                {
                    ["Name"] = "Oren",
                    ["@metadata"] = new DynamicJsonValue
                    {
                        ["Raven-Entity-Name"] = "Users"
                    }
                }, "users/1", BlittableJsonDocumentBuilder.UsageMode.ToDisk))
                {

                    Assert.Throws<ConcurrencyException>(() => _documentsStorage.Put(ctx, "users/1", 1, doc));
                }

                ctx.Transaction.Commit();
            }
        }

        [Fact]
        public void WillVerifyEtags_Existing()
        {
            using (var ctx = new RavenOperationContext(_unmanagedBuffersPool))
            {
                ctx.Transaction = _documentsStorage.Environment.WriteTransaction();

                using (var doc =  ctx.ReadObject(new DynamicJsonValue
                {
                    ["Name"] = "Oren",
                    ["@metadata"] = new DynamicJsonValue
                    {
                        ["Raven-Entity-Name"] = "Users"
                    }
                }, "users/1", BlittableJsonDocumentBuilder.UsageMode.ToDisk))
                {
                    _documentsStorage.Put(ctx, "users/1", null, doc);
                    Assert.Throws<ConcurrencyException>(() => _documentsStorage.Put(ctx, "users/1", 3, doc));
                }

                ctx.Transaction.Commit();
            }
        }

        [Fact]
        public void WillVerifyEtags_OnDeleteExisting()
        {
            using (var ctx = new RavenOperationContext(_unmanagedBuffersPool))
            {
                ctx.Transaction = _documentsStorage.Environment.WriteTransaction();

                using (var doc =  ctx.ReadObject(new DynamicJsonValue
                {
                    ["Name"] = "Oren",
                    ["@metadata"] = new DynamicJsonValue
                    {
                        ["Raven-Entity-Name"] = "Users"
                    }
                }, "users/1", BlittableJsonDocumentBuilder.UsageMode.ToDisk))
                {
                    _documentsStorage.Put(ctx, "users/1", null, doc);
                    Assert.Throws<ConcurrencyException>(() => _documentsStorage.Delete(ctx, "users/1", 3));
                }

                ctx.Transaction.Commit();
            }
        }

        [Fact]
        public void WillVerifyEtags_OnDeleteNotThere()
        {
            using (var ctx = new RavenOperationContext(_unmanagedBuffersPool))
            {
                ctx.Transaction = _documentsStorage.Environment.WriteTransaction();

                Assert.Throws<ConcurrencyException>(() => _documentsStorage.Delete(ctx, "users/1", 3));

                ctx.Transaction.Commit();
            }
        }

        [Fact]
        public void WillVerifyEtags_ShouldBeNew()
        {
            using (var ctx = new RavenOperationContext(_unmanagedBuffersPool))
            {
                ctx.Transaction = _documentsStorage.Environment.WriteTransaction();

                using (var doc =  ctx.ReadObject(new DynamicJsonValue
                {
                    ["Name"] = "Oren",
                    ["@metadata"] = new DynamicJsonValue
                    {
                        ["Raven-Entity-Name"] = "Users"
                    }
                }, "users/1", BlittableJsonDocumentBuilder.UsageMode.ToDisk))
                {
                    _documentsStorage.Put(ctx, "users/1", null, doc);
                    Assert.Throws<ConcurrencyException>(() => _documentsStorage.Put(ctx, "users/1", 0, doc));
                }

                ctx.Transaction.Commit();
            }
        }

        [Fact]
        public void PutDocumentWithoutId()
        {
            var key = "users/";
            using (var ctx = new RavenOperationContext(_unmanagedBuffersPool))
            {
                ctx.Transaction = _documentsStorage.Environment.WriteTransaction();

                for (int i = 1; i < 5; i++)
                {
                    using (var doc =  ctx.ReadObject(new DynamicJsonValue
                    {
                        ["ThisDocId"] = $"{i}"
                    }, key, BlittableJsonDocumentBuilder.UsageMode.ToDisk))
                    {
                        var putResult = _documentsStorage.Put(ctx, key, null, doc);
                        Assert.Equal(i, putResult.ETag);
                    }
                }
                ctx.Transaction.Commit();
            }

            using (var ctx = new RavenOperationContext(_unmanagedBuffersPool))
            {
                ctx.Transaction = _documentsStorage.Environment.WriteTransaction();

                _documentsStorage.Delete(ctx, "users/2", null);

                ctx.Transaction.Commit();
            }

            using (var ctx = new RavenOperationContext(_unmanagedBuffersPool))
            {
                ctx.Transaction = _documentsStorage.Environment.WriteTransaction();

                using (var doc =  ctx.ReadObject(new DynamicJsonValue
                {
                    ["ThisDocId"] = "2"
                }, key, BlittableJsonDocumentBuilder.UsageMode.ToDisk))
                {
                    var putResult = _documentsStorage.Put(ctx, key, null, doc);
                    Assert.Equal(5, putResult.ETag);
                    Assert.Equal("users/5", putResult.Key);
                }
                ctx.Transaction.Commit();
            }


        }

        public void Dispose()
        {
            _documentsStorage.Dispose();
            _unmanagedBuffersPool.Dispose();
        }
    }
}