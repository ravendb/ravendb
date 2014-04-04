using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Raven.Client;
using Raven.Client.Connection;
using Raven.Client.Document;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_1226 : RavenTest
    {
        private const int DummyDocumentCount = 10;
        private const int TestPageSize = 3;

        private class DummyDocument
        {
            public string Str { get; set; }

            public int Num { get; set; }

            private bool Equals(DummyDocument other)
            {
                return string.Equals(Str, other.Str) && Num == other.Num;
            }

            public override bool Equals(object obj)
            {
                if (ReferenceEquals(null, obj)) return false;
                if (ReferenceEquals(this, obj)) return true;
                if (obj.GetType() != GetType()) return false;
                return Equals((DummyDocument) obj);
            }

            public override int GetHashCode()
            {
                unchecked
                {
                    return ((Str != null ? Str.GetHashCode() : 0)*397) ^ Num;
                }
            }
        }

        [Fact]
        public void StreamDocCalled_PageSize_NotIgnored()
        {
            ValidateConstantValues();

            int[] streamDocsRequestCounter = {0};
            using (var ravenServer = GetNewServer())
            {
				ravenServer.Server.RequestManager.BeforeRequest += (sender, args) =>
				{
					if (args.Controller.InnerRequest.RequestUri.PathAndQuery.Contains("/streams/docs?"))
					{
						Interlocked.Increment(ref streamDocsRequestCounter[0]);
					}
				};

                using (var documentStore = NewRemoteDocumentStore(ravenDbServer: ravenServer))
                {
                    AddDummyDocuments(documentStore);

                    var fetchedDocumentsCollection1 = new HashSet<DummyDocument>();

                    using (var docStream = documentStore.DatabaseCommands.StreamDocs(start:0,pageSize: TestPageSize))
                    {
                        while (docStream.MoveNext())
                        {
                            Assert.NotNull(docStream.Current);
                            fetchedDocumentsCollection1.Add(docStream.Current.Deserialize<DummyDocument>(new DocumentConvention()));
                        }

                        Assert.Equal(TestPageSize, fetchedDocumentsCollection1.Count);
                        Assert.Equal(1, streamDocsRequestCounter[0]);
                    }

                    var fetchedDocumentsCollection2 = new HashSet<DummyDocument>();
                    using (var docStream = documentStore.DatabaseCommands.StreamDocs(start: TestPageSize - 1, pageSize: TestPageSize))
                    {
                        while (docStream.MoveNext())
                        {
                            Assert.NotNull(docStream.Current);
                            fetchedDocumentsCollection2.Add(docStream.Current.Deserialize<DummyDocument>(new DocumentConvention()));
                        }

                        Assert.Equal(TestPageSize, fetchedDocumentsCollection2.Count);
                    }


                    Assert.Equal(1, fetchedDocumentsCollection1.Intersect(fetchedDocumentsCollection2).Count());
                    Assert.True(fetchedDocumentsCollection1.Last().Equals(fetchedDocumentsCollection2.First()));
                
                }
            }
        }

        private static void ValidateConstantValues()
        {
            if (TestPageSize >= (DummyDocumentCount/2))
// ReSharper disable once CSharpWarnings::CS0162
                throw new ArgumentException("test page size constant must be much smaller than dummy document count");
        }

        private void AddDummyDocuments(IDocumentStore documentStore)
        {
            using (var session = documentStore.OpenSession())
            {
                for (int docIndex = 0; docIndex < DummyDocumentCount; docIndex++)
                {
                    var newDummyDoc = new DummyDocument() {Str = "String #" + docIndex, Num = docIndex};
                    session.Store(newDummyDoc);
                }
                session.SaveChanges();
            }
        }
    }
}
