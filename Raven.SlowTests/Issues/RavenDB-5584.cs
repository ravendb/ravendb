using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Smuggler;
using Raven.Bundles.Versioning.Data;
using Raven.Client.FileSystem.Bundles.Versioning;
using Raven.Database.FileSystem.Bundles.Versioning;
using Raven.Smuggler;
using Raven.Tests.Common;
using Raven.Tests.Helpers;
using Xunit;

namespace Raven.SlowTests.Issues
{
    public class RavenDB_5584: RavenFilesTestBase
    {
        [Fact]
        public async Task BetweenOperation_ShouldNotCreateFilesystem()
        {
            string export = Path.Combine(NewDataPath("export_5584"), "Export");

            using (var store = NewStore())
            {
                var ms = new MemoryStream();
                var buffer = new byte[1024];

                for (int i = 0; i < buffer.Length; i++)
                {
                    buffer[i] = (byte)i;
                }

                ms.Write(buffer, 0, buffer.Length);
                ms.Position = 0;
                await store.AsyncFilesCommands.UploadAsync("file.txt", ms);

                var tasks = new Task[50];

                for (var i = 0; i < 100; i += tasks.Length)
                {
                    for (var j = 0; j < tasks.Length; j++)
                    {
                        var targetNAme = "file" + (i + j).ToString() + ".txt";
                        tasks[j] = store.AsyncFilesCommands.CopyAsync("file.txt", targetNAme);
                    }
                    Task.WaitAll(tasks);
                }
                

                var smugglerApi = new SmugglerFilesApi();


                await smugglerApi.ExportData(exportOptions: new SmugglerExportOptions<FilesConnectionStringOptions>()
                {
                    From = new FilesConnectionStringOptions()
                    {
                        DefaultFileSystem = store.DefaultFileSystem,
                        Url = store.Url,

                    },
                    ToFile = export,

                });


                using (var store2 = NewStore(fileSystemName:"export",index:1))
                {
                    var importSmugglerApi = new SmugglerFilesApi();
                    await importSmugglerApi.ImportData(importOptions: new SmugglerImportOptions<FilesConnectionStringOptions>()
                    {
                        To = new FilesConnectionStringOptions()
                        {
                            DefaultFileSystem = store2.DefaultFileSystem,
                            Url = store2.Url,

                        },
                        FromFile = "c:\\Temp\\export.ravendump",
                    });
                }
            }
        }
    }
}
