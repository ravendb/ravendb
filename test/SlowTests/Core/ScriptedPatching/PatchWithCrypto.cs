using System;
using System.Dynamic;
using System.Collections.Generic;
using System.Text;
using FastTests;
using Orders;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Queries;
using Tests.Infrastructure;
using Xunit;
using System.Linq;

namespace SlowTests.Core.ScriptedPatching;

public class PatchWithCrypto(ITestOutputHelper output) : RavenTestBase(output)
{
    [RavenTheory(RavenTestCategory.ClientApi | RavenTestCategory.Patching)]
    [RavenData(DatabaseMode = RavenDatabaseMode.All)]
    public void CanGenerateGuidViaPatch(Options options)
    {
        using var store = GetDocumentStore(options);

        using (var session = store.OpenSession())
        {
            var doc = new Company { Name = "The Wall" };
            session.Store(doc, "companies/1");
            session.SaveChanges();
        }

        using (var session = store.OpenSession())
        {
            var patch = new PatchCommandData("companies/1", null,
                new PatchRequest
                {
                    Script = "this.ExternalId = crypto.randomUUID();"
                });
            session.Advanced.Defer(patch);
            session.SaveChanges();
        }

        using (var session = store.OpenSession())
        {
            var c = session.Load<Company>("companies/1");

            Guid.Parse(c.ExternalId);
        }
    }

    [RavenTheory(RavenTestCategory.ClientApi | RavenTestCategory.Patching)]
    [RavenData(DatabaseMode = RavenDatabaseMode.All)]
    public void CanGetRandomValues(Options options)
    {
        using var store = GetDocumentStore(options);

        using (var session = store.OpenSession())
        {
            var doc = new Company { Name = "The Wall" };
            session.Store(doc, "companies/1");
            session.SaveChanges();
        }

        using (var session = store.OpenSession())
        {
            var patch = new PatchCommandData("companies/1", null,
                new PatchRequest
                {
                    Script = @"this.Random = crypto.getRandomValuesBase64(32);"
                });
            session.Advanced.Defer(patch);
            session.SaveChanges();
        }

        using (var session = store.OpenSession())
        {
            var c = session.Load<Company>("companies/1");
            Assert.NotNull(c.Random);

            var buf = Convert.FromBase64String(c.Random);
            Assert.Equal(32, buf.Length);
            Assert.False(buf.All(x => x == 0), "Cannot get random string with all zeros");
        }
    }

    [RavenTheory(RavenTestCategory.ClientApi | RavenTestCategory.Patching)]
    [RavenData(DatabaseMode = RavenDatabaseMode.All)]
    public void CanDigest(Options options)
    {
        using var store = GetDocumentStore(options);
        using (var session = store.OpenSession())
        {
            var doc = new Company { Name = "test" };
            session.Store(doc, "companies/1");
            session.SaveChanges();
        }

        using (var session = store.OpenSession())
        {
            // Synchronous digest in ScriptRunner implementation
            var patch = new PatchCommandData("companies/1", null, new PatchRequest
            {
                Script = @"
var data = 'Hello world'; // string is converted to utf8 bytes automatically
var hash = crypto.digest('SHA-256', data);
this.Hash = hash;
"
            });
            session.Advanced.Defer(patch);
            session.SaveChanges();
        }
        
        using (var session = store.OpenSession())
        {
            var c = session.Load<Company>("companies/1");
            var buf = Convert.FromBase64String(c.Hash);
            Assert.Equal(32, buf.Length);
            // Precomputed value for SHA-256("Hello world")
            Assert.Equal("ZOyIygCyaOW6GjVnihtTFtIS9PNmskdyMlNKiuyjfzw=", c.Hash);
        }
    }

    [RavenTheory(RavenTestCategory.ClientApi | RavenTestCategory.Patching)]
    [RavenData(DatabaseMode = RavenDatabaseMode.All)]
    public void CanSignAndVerify(Options options)
    {
        using var store = GetDocumentStore(options);
        using (var session = store.OpenSession())
        {
            var doc = new Company { Name = "test" };
            session.Store(doc, "companies/1");
            session.SaveChanges();
        }

        using (var session = store.OpenSession())
        {
            var patch = new PatchCommandData("companies/1", null, new PatchRequest
            {
                Script = @"
var key = crypto.getRandomValuesBase64(64); // 512 bits key as base64
var data = 'some message';

// Sign - returns base64 string
var sig = crypto.sign('SHA-256', key, data);
this.Signature = sig;

// Verify with base64 signature
var valid = crypto.verify('SHA-256', key, sig, data);
this.Valid = valid;

// Verify invalid
var invalid = crypto.verify('SHA-256', key, sig, 'other');
this.Invalid = invalid;
"
            });
            session.Advanced.Defer(patch);
            session.SaveChanges();
        }

        using (var session = store.OpenSession())
        {
            var c = session.Load<Company>("companies/1");
            Assert.True(c.Valid);
            Assert.False(c.Invalid);
            Assert.NotEmpty(c.Signature);
        }
    }

    [RavenTheory(RavenTestCategory.ClientApi | RavenTestCategory.Patching)]
    [RavenData(DatabaseMode = RavenDatabaseMode.All)]
    public void CanEncryptAndDecrypt(Options options)
    {
        using var store = GetDocumentStore(options);
        using (var session = store.OpenSession())
        {
            var doc = new Company { Name = "test" };
            session.Store(doc, "companies/1");
            session.Advanced.GetMetadataFor(doc).Remove("Raven-Clr-Type");
            session.SaveChanges();
        }

        using (var session = store.OpenSession())
        {
            var patch = new PatchCommandData("companies/1", null, new PatchRequest
            {
                Script = @"
var key = crypto.getRandomValuesBase64(32); // 256 bits as base64
var iv = crypto.getRandomValuesBase64(12);  // 96 bits as base64

var plain = 'Secret Message';
var encrypted = crypto.encryptAesGcm(iv, key, plain);
this.Encrypted = encrypted;

this.DecryptedString = crypto.decryptAesGcm(iv, key, encrypted, 'string');
"
                });
                session.Advanced.Defer(patch);
                session.SaveChanges();
            }

        using (var session = store.OpenSession())
        {
            var c = session.Load<dynamic>("companies/1");
            var s = (string)c.DecryptedString;
            Assert.Equal("Secret Message", s);
        }
    }


    private class Company 
    {
        public string ExternalId { get; set; }
        public string Name { get; set; }
        public string Random { get; set; }
        public string Hash { get; set; }
        public string Signature { get; set; }
        public string Encrypted { get; set; }
        public bool Valid { get; set; }
        public bool Invalid { get; set; }
        public string DecryptedString { get; set; }
    }
}

