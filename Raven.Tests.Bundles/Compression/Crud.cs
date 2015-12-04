using System;
using System.IO;

using Raven.Abstractions.Data;
using Raven.Json.Linq;
using Raven.Tests.Common;
using Raven.Tests.Common.Util;

using Xunit;

namespace Raven.Tests.Bundles.Compression
{
    public class Crud : Compression
    {
        
        [Fact]
        public void StoreAndLoad()
        {
            const string CompanyName = "Company Name";
            var company = new Company { Name = CompanyName };
            using (var session = documentStore.OpenSession())
            {
                session.Store(company);
                session.SaveChanges();
            }

            using (var session = documentStore.OpenSession())
            {
                Assert.Equal(CompanyName, session.Load<Company>(1).Name);
            }

            AssertPlainTextIsNotSavedInDatabase_ExceptIndexes(CompanyName);
        }
    }

    public class CrudWithDtc : RavenTest
    {
        [Fact]
        public void Transactional()
        {
            var compressedDataPath = NewDataPath("Compressed");

            const string FirstCompany = "FirstCompany";

            using (var server = GetNewServer(activeBundles: "Compression", dataDirectory: compressedDataPath, runInMemory: false, requestedStorage: "esent"))
            using (var documentStore = NewRemoteDocumentStore(ravenDbServer: server))
            {
                // write in transaction
                documentStore.DatabaseCommands.Put("docs/1", null, new RavenJObject { { "Name", FirstCompany } }, new RavenJObject { { "Raven-Transaction-Information", Guid.NewGuid() + ", " + TimeSpan.FromMinutes(1) } });

                var jsonDocument = documentStore.DatabaseCommands.Get("docs/1");
                Assert.True(jsonDocument.Metadata.Value<bool>(Constants.RavenDocumentDoesNotExists));
            }

            EncryptionTestUtil.AssertPlainTextIsNotSavedInAnyFileInPath(new [] {FirstCompany}, compressedDataPath, file => Path.GetExtension(file) != ".cfs");
        }
    }
}
