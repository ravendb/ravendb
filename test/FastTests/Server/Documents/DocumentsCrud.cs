using System.Linq;
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Configuration.Memory;
using Raven.Abstractions.Exceptions;
using Raven.Server.Config;
using Raven.Server.Documents;
using Raven.Server.Json;
using Raven.Server.Json.Parsing;
using Voron;
using Xunit;

namespace BlittableTests.Documents
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
        public async Task PutAndGetDocumentById(string key)
        {
            using (var ctx = new RavenOperationContext(_unmanagedBuffersPool))
            {
                ctx.Transaction = _documentsStorage.Environment.WriteTransaction();

                using (var doc = await ctx.ReadObject(new DynamicJsonValue
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
        public async Task CanDelete(string key)
        {
            using (var ctx = new RavenOperationContext(_unmanagedBuffersPool))
            {
                ctx.Transaction = _documentsStorage.Environment.WriteTransaction();

                using (var doc = await ctx.ReadObject(new DynamicJsonValue
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
        public async Task CanQueryByGlobalEtag()
        {
            using (var ctx = new RavenOperationContext(_unmanagedBuffersPool))
            {
                ctx.Transaction = _documentsStorage.Environment.WriteTransaction();

                using (var doc = await ctx.ReadObject(new DynamicJsonValue
                {
                    ["Name"] = "Oren",
                    ["@metadata"] = new DynamicJsonValue
                    {
                        ["Raven-Entity-Name"] ="Users"
                    }
                }, "users/1", BlittableJsonDocumentBuilder.UsageMode.ToDisk))
                {
                    _documentsStorage.Put(ctx, "users/1", null, doc);
                }
                using (var doc = await ctx.ReadObject(new DynamicJsonValue
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
                using (var doc = await ctx.ReadObject(new DynamicJsonValue
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

                var documents = _documentsStorage.GetDocumentsAfter(ctx, 0).ToList();
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
            using (var ctx = new RavenOperationContext(_unmanagedBuffersPool))
            {
                ctx.Transaction = _documentsStorage.Environment.WriteTransaction();

                using (var doc = await ctx.ReadObject(new DynamicJsonValue
                {
                    ["Name"] = "Oren",
                    ["@metadata"] = new DynamicJsonValue
                    {
                        ["Raven-Entity-Name"] = "Users"
                    }
                }, "users/1", BlittableJsonDocumentBuilder.UsageMode.ToDisk))
                {
                    var etag = _documentsStorage.Put(ctx, "users/1", null, doc);
                    Assert.Equal(1, etag);
                }
               
                ctx.Transaction.Commit();
            }

            Restart();

            using (var ctx = new RavenOperationContext(_unmanagedBuffersPool))
            {
                ctx.Transaction = _documentsStorage.Environment.WriteTransaction();

                using (var doc = await ctx.ReadObject(new DynamicJsonValue
                {
                    ["Name"] = "Oren",
                    ["@metadata"] = new DynamicJsonValue
                    {
                        ["Raven-Entity-Name"] = "Users"
                    }
                }, "users/2", BlittableJsonDocumentBuilder.UsageMode.ToDisk))
                {
                    var etag = _documentsStorage.Put(ctx, "users/2", null, doc);
                    Assert.Equal(2, etag);
                }

                ctx.Transaction.Commit();
            }
        }

        [Fact]
        public async Task EtagsArePersistedWithDeletes()
        {
            using (var ctx = new RavenOperationContext(_unmanagedBuffersPool))
            {
                ctx.Transaction = _documentsStorage.Environment.WriteTransaction();

                using (var doc = await ctx.ReadObject(new DynamicJsonValue
                {
                    ["Name"] = "Oren",
                    ["@metadata"] = new DynamicJsonValue
                    {
                        ["Raven-Entity-Name"] = "Users"
                    }
                }, "users/1", BlittableJsonDocumentBuilder.UsageMode.ToDisk))
                {
                    var etag = _documentsStorage.Put(ctx, "users/1", null, doc);
                    Assert.Equal(1, etag);
                    _documentsStorage.Delete(ctx, "users/1", null);
                }
                
                ctx.Transaction.Commit();
            }

            Restart();

            using (var ctx = new RavenOperationContext(_unmanagedBuffersPool))
            {
                ctx.Transaction = _documentsStorage.Environment.WriteTransaction();
                _documentsStorage.ReadLastEtag(ctx.Transaction);
                using (var doc = await ctx.ReadObject(new DynamicJsonValue
                {
                    ["Name"] = "Oren",
                    ["@metadata"] = new DynamicJsonValue
                    {
                        ["Raven-Entity-Name"] = "Users"
                    }
                }, "users/2", BlittableJsonDocumentBuilder.UsageMode.ToDisk))
                {
                    var etag = _documentsStorage.Put(ctx, "users/2", null, doc);
                    Assert.Equal(2, etag);
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
        public async Task CanQueryByPrefix()
        {
            using (var ctx = new RavenOperationContext(_unmanagedBuffersPool))
            {
                ctx.Transaction = _documentsStorage.Environment.WriteTransaction();

                using (var doc = await ctx.ReadObject(new DynamicJsonValue
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
                using (var doc = await ctx.ReadObject(new DynamicJsonValue
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
                using (var doc = await ctx.ReadObject(new DynamicJsonValue
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

                var documents = _documentsStorage.GetDocumentsStartingWith(ctx, "users/").ToList();
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
        public async Task CanQueryByCollectionEtag()
        {
            using (var ctx = new RavenOperationContext(_unmanagedBuffersPool))
            {
                ctx.Transaction = _documentsStorage.Environment.WriteTransaction();

                using (var doc = await ctx.ReadObject(new DynamicJsonValue
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
              
                using (var doc = await ctx.ReadObject(new DynamicJsonValue
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
                using (var doc = await ctx.ReadObject(new DynamicJsonValue
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

                var documents = _documentsStorage.GetDocumentsAfter(ctx, "Users", 0).ToList();
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
        public async Task WillVerifyEtags_New()
        {
            using (var ctx = new RavenOperationContext(_unmanagedBuffersPool))
            {
                ctx.Transaction = _documentsStorage.Environment.WriteTransaction();

                using (var doc = await ctx.ReadObject(new DynamicJsonValue
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
        public async Task WillVerifyEtags_Existing()
        {
            using (var ctx = new RavenOperationContext(_unmanagedBuffersPool))
            {
                ctx.Transaction = _documentsStorage.Environment.WriteTransaction();

                using (var doc = await ctx.ReadObject(new DynamicJsonValue
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
        public async Task WillVerifyEtags_OnDeleteExisting()
        {
            using (var ctx = new RavenOperationContext(_unmanagedBuffersPool))
            {
                ctx.Transaction = _documentsStorage.Environment.WriteTransaction();

                using (var doc = await ctx.ReadObject(new DynamicJsonValue
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
        public async Task WillVerifyEtags_ShouldBeNew()
        {
            using (var ctx = new RavenOperationContext(_unmanagedBuffersPool))
            {
                ctx.Transaction = _documentsStorage.Environment.WriteTransaction();

                using (var doc = await ctx.ReadObject(new DynamicJsonValue
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

        public void Dispose()
        {
            _documentsStorage.Dispose();
            _unmanagedBuffersPool.Dispose();
        }
    }
}