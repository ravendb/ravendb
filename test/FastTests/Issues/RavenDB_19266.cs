using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Client.ServerWide.Operations.DocumentsCompression;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Issues
{
    public class RavenDB_19266 : RavenTestBase
    {
        public RavenDB_19266(ITestOutputHelper output) : base(output) { }

        public static string RandomString(Random random, int length)
        {
            const string chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
            return new string(Enumerable.Repeat(chars, length)
                .Select(s => s[random.Next(s.Length)]).ToArray());
        }

        internal class Order
        {
            public List<string> Lines = new();
        }

        [Theory]
        [InlineData(3)]
        [InlineData(6)]
        [InlineData(10)]
        public void CompressAndDecompressDocument(int size)
        {
            using (var store = GetDocumentStore())
            {
                var documentsCompression = new DocumentsCompressionConfiguration(true, true, "Orders");
                store.Maintenance.Send(new UpdateDocumentsCompressionConfigurationOperation(documentsCompression));
                
                using (var session = store.OpenSession())
                {
                    var doc = new Order();

                    for (int i = 0; i < size * 1024; i++)
                    {
                        string line = RandomString(new Random(123), 1024);
                        doc.Lines.Add(line);
                    }
                    
                    session.Store(doc);
                    session.SaveChanges();
                }
            }
        }
    }
}
