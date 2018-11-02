// -----------------------------------------------------------------------
//  <copyright file="TouchFilesTests.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.IO;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Util;
using Raven.Json.Linq;
using Raven.Tests.Helpers;
using Xunit;

namespace Raven.Tests.FileSystem
{
    public class TouchFilesTests : RavenFilesTestBase
    {
        [Fact]
        public async Task CanTouchBatchOfFiles()
        {
            var client = NewAsyncClient();

            var numberOfFiles = 53;

            for (int i = 0; i < numberOfFiles; i++)
            {
                await client.UploadAsync($"/my/files/{i}", new MemoryStream(), new RavenJObject
                {
                    {"Number", i}
                });
            }

            var stats = await client.GetStatisticsAsync();

            var start = Etag.Empty;
            long skipped = 0;
            long modifed = 0;

            while (EtagUtil.IsGreaterThan(stats.LastFileEtag, start))
            {
                var result = await client.TouchFilesAsync(start, 10);

                start = result.LastProcessedFileEtag;

                modifed += result.NumberOfProcessedFiles;
                skipped += result.NumberOfFilteredFiles;
            }

            Assert.True(modifed >= numberOfFiles);
            Assert.Equal(0, skipped);
        }
    }
}