using System;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Indexes;
using Raven.Embedded;
using Xunit;

namespace EmbeddedTests.Issues
{
    // runs for every EmbeddedTests target: on net11.0 array.Max()/array.Min() bind to MemoryExtensions.Max/Min(ReadOnlySpan<T>),
    // on net10.0 and net472 (the netstandard2.0 client) to Enumerable.Max/Min, and the client has to translate both
    public class RavenDB_27833 : EmbeddedTestBase
    {
        [Fact]
        public void ArrayMaxAndMin_InProjection_ShouldTranslateToJavaScript()
        {
            using (var embedded = new EmbeddedServer())
            {
                embedded.StartServer(CopyServerAndCreateOptions());

                using (var store = embedded.GetDocumentStore(nameof(ArrayMaxAndMin_InProjection_ShouldTranslateToJavaScript)))
                {
                    StoreUser(store);

                    using (var session = store.OpenSession())
                    {
                        var query = from u in session.Query<User>()
                                    select new { MaxRole = u.Roles.Max(), MinRole = u.Roles.Min(), MaxScore = u.Scores.Max(), MinScore = u.Scores.Min() };

                        var rql = query.ToString();
                        Assert.Contains("u.Roles.reduce((a, b) => Raven_Max(a, b))", rql);
                        Assert.Contains("u.Scores.reduce((a, b) => Raven_Min(a, b))", rql);
                        Assert.DoesNotContain("ReadOnlySpan", rql);

                        var result = query.Single();
                        Assert.Equal("viewer", result.MaxRole);
                        Assert.Equal("admin", result.MinRole);
                        Assert.Equal(7, result.MaxScore);
                        Assert.Equal(-2, result.MinScore);
                    }
                }
            }
        }

        [Fact]
        public void ArrayMaxAndMin_InIndexDefinition_ShouldTranslateToDynamicEnumerable()
        {
            var map = new Users_RolesAndScores { Conventions = new DocumentConventions() }.CreateIndexDefinition().Maps.Single();
            Assert.Contains("DynamicEnumerable.Max(u.Roles)", map);
            Assert.Contains("DynamicEnumerable.Min(u.Scores)", map);
            Assert.DoesNotContain("MemoryExtensions", map);
            Assert.DoesNotContain("ReadOnlySpan", map);

            using (var embedded = new EmbeddedServer())
            {
                embedded.StartServer(CopyServerAndCreateOptions());

                using (var store = embedded.GetDocumentStore(nameof(ArrayMaxAndMin_InIndexDefinition_ShouldTranslateToDynamicEnumerable)))
                {
                    new Users_RolesAndScores().Execute(store);
                    StoreUser(store);

                    using (var session = store.OpenSession())
                    {
                        var result = session.Query<Users_RolesAndScores.Result, Users_RolesAndScores>()
                            .Customize(x => x.WaitForNonStaleResults())
                            .ProjectInto<Users_RolesAndScores.Result>()
                            .Single();

                        Assert.Equal("viewer", result.MaxRole);
                        Assert.Equal("admin", result.MinRole);
                        Assert.Equal(7, result.MaxScore);
                        Assert.Equal(-2, result.MinScore);
                    }
                }
            }
        }

        [Fact]
        public void Secured_ShouldLoadPfxAndCertificateFiles()
        {
            var directory = NewDataPath();
            Directory.CreateDirectory(directory);

            using (var rsa = RSA.Create(2048))
            {
                var request = new CertificateRequest("CN=RavenDB_27833", rsa, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
                using (var source = request.CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(1)))
                {
                    var pfxPath = Path.Combine(directory, "client.pfx");
                    File.WriteAllBytes(pfxPath, source.Export(X509ContentType.Pfx, "secret"));
                    var cerPath = Path.Combine(directory, "client.cer");
                    File.WriteAllBytes(cerPath, source.Export(X509ContentType.Cert));

                    var pfx = new ServerOptions().Secured(pfxPath, "secret").Security;
                    Assert.Equal(source.Thumbprint, pfx.ServerCertificateThumbprint);
                    Assert.True(pfx.ClientCertificate.HasPrivateKey);

                    var cer = new ServerOptions().Secured(cerPath).Security;
                    Assert.Equal(source.Thumbprint, cer.ServerCertificateThumbprint);
                    Assert.False(cer.ClientCertificate.HasPrivateKey);

                    Assert.ThrowsAny<CryptographicException>(() => new ServerOptions().Secured(pfxPath, "wrong"));
                }
            }
        }

        private static void StoreUser(IDocumentStore store)
        {
            using (var session = store.OpenSession())
            {
                session.Store(new User { Roles = new[] { "editor", "admin", "viewer" }, Scores = new[] { 3, -2, 7 } });
                session.SaveChanges();
            }
        }

        private class User
        {
            public string Id { get; set; }

            public string[] Roles { get; set; }

            public int[] Scores { get; set; }
        }

        private class Users_RolesAndScores : AbstractIndexCreationTask<User, Users_RolesAndScores.Result>
        {
            public class Result
            {
                public string MaxRole { get; set; }

                public string MinRole { get; set; }

                public int MaxScore { get; set; }

                public int MinScore { get; set; }
            }

            public Users_RolesAndScores()
            {
                Map = users => from u in users
                               select new Result { MaxRole = u.Roles.Max(), MinRole = u.Roles.Min(), MaxScore = u.Scores.Max(), MinScore = u.Scores.Min() };

                StoreAllFields(FieldStorage.Yes);
            }
        }
    }
}
