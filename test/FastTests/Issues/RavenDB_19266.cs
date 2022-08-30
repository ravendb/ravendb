using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
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

        [Fact]
        public void CompressAndDecompressDocument3Mb()
        {
            using (var store = GetDocumentStore())
            {
                var record = store.Maintenance.Server.Send(new GetDatabaseRecordOperation(store.Database));
                record.DocumentsCompression = new DocumentsCompressionConfiguration(true, true, "Orders");
                store.Maintenance.Server.Send(new UpdateDatabaseOperation(record, record.Etag));
                
                using (var session = store.OpenSession())
                {
                    var doc = new Order();

                    for (int i = 0; i < 3 * 1024; i++)
                    {
                        string line = RandomString(new Random(123), 1024);
                        doc.Lines.Add(line);
                    }
                    
                    session.Store(doc);
                    session.SaveChanges();
                }
            }
        }

        [Fact]
        public void CompressAndDecompressDocument6Mb()
        {
            using (var store = GetDocumentStore())
            {
                var record = store.Maintenance.Server.Send(new GetDatabaseRecordOperation(store.Database));
                record.DocumentsCompression = new DocumentsCompressionConfiguration(true, true, "Orders");
                store.Maintenance.Server.Send(new UpdateDatabaseOperation(record, record.Etag));

                using (var session = store.OpenSession())
                {
                    var doc = new Order();

                    for (int i = 0; i < 6 * 1024; i++)
                    {
                        string line = RandomString(new Random(123), 1024);
                        doc.Lines.Add(line);
                    }

                    session.Store(doc);
                    session.SaveChanges();
                }
            }
        }

        [Fact]
        public void CompressAndDecompressDocument10Mb()
        {
            using (var store = GetDocumentStore())
            {
                var record = store.Maintenance.Server.Send(new GetDatabaseRecordOperation(store.Database));
                record.DocumentsCompression = new DocumentsCompressionConfiguration(true, true, "Orders");
                store.Maintenance.Server.Send(new UpdateDatabaseOperation(record, record.Etag));

                using (var session = store.OpenSession())
                {
                    var doc = new Order();

                    for (int i = 0; i < 10 * 1024; i++)
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
