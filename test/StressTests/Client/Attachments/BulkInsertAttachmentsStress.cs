using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Tests.Core.Utils.Entities;
using SlowTests.Client.Attachments;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace StressTests.Client.Attachments
{
    public class BulkInsertAttachmentsStress : NoDisposalNoOutputNeeded
    {
        public BulkInsertAttachmentsStress(ITestOutputHelper output) : base(output)
        {
        }

        [Theory]
        [InlineData(100, 32 * 1024 * 1024)]
        public async Task StoreManyAttachmentsStress(int count, int size)
        {
            using (var test = new BulkInsertAttachments(Output))
            {
                await test.StoreManyAttachments(count, size);
            }
        }

        [Theory]
        [InlineData(1000, 100, 32 * 1024)]
        [InlineData(1000, 100, 64 * 1024)]
        public async Task StoreManyAttachmentsAndDocsStress(int count, int attachments, int size)
        {
            using (var test = new BulkInsertAttachments(Output))
            {
                await test.StoreManyAttachmentsAndDocs(count, attachments, size);
            }
        }
    }
}
