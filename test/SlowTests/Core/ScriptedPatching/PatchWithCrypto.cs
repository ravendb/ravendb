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
using Xunit.Abstractions;
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
                    Script = @"
var a = new Uint8Array(32);
crypto.getRandomValues(a);
this.Random = a.toBase64();"
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
var hash = crypto.subtle.digest('SHA-256', data);
this.Hash = new Uint8Array(hash).toBase64();
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
var key = new Uint8Array(64); // 512 bits key
crypto.getRandomValues(key);
var data = 'some message';

// Sign
var sig = crypto.subtle.sign({name: 'HMAC', hash: 'SHA-256'}, key, data);
this.Signature = new Uint8Array(sig).toBase64();

// Verify
var valid = crypto.subtle.verify({name: 'HMAC', hash: 'SHA-256'}, key, sig, data);
this.Valid = valid;

// Verify invalid
var invalid = crypto.subtle.verify({name: 'HMAC', hash: 'SHA-256'}, key, sig, 'other');
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
var key = new Uint8Array(32); // 256 bits
crypto.getRandomValues(key);
var iv = new Uint8Array(12);
crypto.getRandomValues(iv);

var plain = 'Secret Message';
var encrypted = crypto.subtle.encrypt({name: 'AES-GCM', iv: iv}, key, plain);
this.Encrypted = Array.from(new Uint8Array(encrypted));

var decrypted = crypto.subtle.decrypt({name: 'AES-GCM', iv: iv}, key, encrypted);
this.DecryptedLen = decrypted.byteLength;

try {
    var decoder = new TextDecoder();
    this.DecodedResult = decoder.decode(decrypted);
    this.DecoderError = null;
} catch(e) {
    this.DecoderError = e.toString();
    this.DecodedResult = null;
}
this.DecryptedString = this.DecodedResult;
"
                });
                session.Advanced.Defer(patch);
                session.SaveChanges();
            }

        using (var session = store.OpenSession())
        {
            var c = session.Load<dynamic>("companies/1");
            string error = c.DecoderError;
            Assert.True(string.IsNullOrEmpty(error), "Script error: " + error);

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

