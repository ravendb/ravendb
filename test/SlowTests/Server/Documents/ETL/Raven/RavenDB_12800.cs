using Tests.Infrastructure;
using System;
using System.IO;
using System.Linq;
using Raven.Server.Config;
using Raven.Server.Documents.ETL.Providers.Raven;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.ETL.Raven
{
    public class RavenDB_12800 : EtlTestBase
    {
        public RavenDB_12800(ITestOutputHelper output) : base(output)
        {
        }

        private const string scriptShould_stop_batch_if_size_limit_exceeded = @"
            var doc = loadToUsers(this);

            var attachments = getAttachments();

            for (var i = 0; i < attachments.length; i++) {
                doc.addAttachment(loadAttachment(attachments[i].Name));
            }
        ";

        [Theory]
        [RavenData(@"
            var doc = loadToUsers(this);
            var attachments = getAttachments();
            for (var i = 0; i < attachments.length; i++) {
                doc.addAttachment(loadAttachment(attachments[i].Name));
            }
        ", JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        [RavenData(null, JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public void Should_stop_batch_if_size_limit_exceeded(Options options, string script)
        {
            var op = options.Clone();
            options.ModifyDatabaseRecord = x => x.Settings[RavenConfiguration.GetKey(c => c.Etl.MaxBatchSize)] = "5";
            using (var src = GetDocumentStore(options))
            using (var dest = GetDocumentStore(op))
            {
                using (var session = src.OpenSession())
                {

                    for (int i = 0; i < 6; i++)
                    {
                        var user = new User();
                        session.Store(user);

                        var r = new Random(i);

                        var bytes = new byte[1024 * 1024 * 1];

                        r.NextBytes(bytes);

                        session.Advanced.Attachments.Store(user, "my-attachment", new MemoryStream(bytes));
                    }

                    session.SaveChanges();
                }

                AddEtl(src, dest, "Users", script: script);

                var etlDone = WaitForEtl(src, (n, s) => s.LoadSuccesses >= 5);

                etlDone.Wait(TimeSpan.FromMinutes(1));

                var database = GetDatabase(src.Database).Result;

                var etlProcess = (RavenEtl)database.EtlLoader.Processes.First();

                var stats = etlProcess.GetPerformanceStats();

                Assert.Contains("Stopping the batch because maximum batch size limit was reached (5 MBytes)", stats.Select(x => x.BatchTransformationCompleteReason).ToList());

                etlDone = WaitForEtl(src, (n, s) => s.LoadSuccesses >= 6);

                etlDone.Wait(TimeSpan.FromMinutes(1));
            }
        }
    }
}
