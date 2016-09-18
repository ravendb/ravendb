// -----------------------------------------------------------------------
//  <copyright file="FsStudioTasksController.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading;
using System.Threading.Tasks;
using System.Web.Http;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Json;
using Raven.Abstractions.Smuggler;
using Raven.Abstractions.Util;
using Raven.Database.FileSystem.Actions;
using Raven.Database.FileSystem.Smuggler;
using Raven.Database.Server.Controllers;
using Raven.Database.Server.WebApi.Attributes;
using Raven.Json.Linq;

namespace Raven.Database.FileSystem.Controllers
{
    public class FsStudioTasksController : RavenFsApiController
    {
        [HttpPost]
        [RavenRoute("fs/{fileSystemName}/studio-tasks/import")]
        public async Task<HttpResponseMessage> ImportFilesystem(int batchSize)
        {
            if (!Request.Content.IsMimeMultipartContent())
            {
                throw new HttpResponseException(HttpStatusCode.UnsupportedMediaType);
            }

            string tempPath = Path.GetTempPath();
            var fullTempPath = tempPath + Constants.TempUploadsDirectoryName;
            if (File.Exists(fullTempPath))
                File.Delete(fullTempPath);
            if (Directory.Exists(fullTempPath) == false)
                Directory.CreateDirectory(fullTempPath);

            var streamProvider = new MultipartFileStreamProvider(fullTempPath);
            await Request.Content.ReadAsMultipartAsync(streamProvider).ConfigureAwait(false);
            var uploadedFilePath = streamProvider.FileData[0].LocalFileName;

            string fileName = null;
            var fileContent = streamProvider.Contents.SingleOrDefault();
            if (fileContent != null)
            {
                fileName = fileContent.Headers.ContentDisposition.FileName.Replace("\"", string.Empty);
            }

            var status = new ImportOperationStatus();
            var cts = new CancellationTokenSource();

            var task = Task.Run(async () =>
            {
                try
                {
                    var dataDumper = new FilesystemDataDumper(FileSystem);
                    dataDumper.Progress += s => status.LastProgress = s;
                    var smugglerOptions = dataDumper.Options;
                    smugglerOptions.BatchSize = batchSize;
                    smugglerOptions.CancelToken = cts;

                    await dataDumper.ImportData(new SmugglerImportOptions<FilesConnectionStringOptions> { FromFile = uploadedFilePath });
                }
                catch (Exception e)
                {
                    status.Faulted = true;
                    status.State = RavenJObject.FromObject(new
                    {
                        Error = e.ToString()
                    });
                    if (cts.Token.IsCancellationRequested)
                    {
                        status.State = RavenJObject.FromObject(new { Error = "Task was cancelled" });
                        cts.Token.ThrowIfCancellationRequested(); //needed for displaying the task status as canceled and not faulted
                    }

                    if (e is InvalidDataException)
                    {
                        status.ExceptionDetails = e.Message;
                    }
                    else
                    {
                        status.ExceptionDetails = e.ToString();
                    }
                    throw;
                }
                finally
                {
                    status.Completed = true;
                    File.Delete(uploadedFilePath);
                }
            }, cts.Token);

            long id;
            FileSystem.Tasks.AddTask(task, status, new TaskActions.PendingTaskDescription
            {
                StartTime = SystemTime.UtcNow,
                TaskType = TaskActions.PendingTaskType.ImportFileSystem,
                Payload = fileName,

            }, out id, cts);

            return GetMessageWithObject(new
            {
                OperationId = id
            });
        }

        [HttpPost]
        [RavenRoute("fs/{fileSystemName}/studio-tasks/exportFilesystem")]
        public Task<HttpResponseMessage> ExportFilesystem(StudioTasksController.ExportData smugglerOptionsJson)
        {
            var requestString = smugglerOptionsJson.SmugglerOptions;
            SmugglerFilesOptions smugglerOptions;

            using (var jsonReader = new RavenJsonTextReader(new StringReader(requestString)))
            {
                var serializer = JsonExtensions.CreateDefaultJsonSerializer();
                smugglerOptions = (SmugglerFilesOptions)serializer.Deserialize(jsonReader, typeof(SmugglerFilesOptions));
            }


            var result = GetEmptyMessage();

            // create PushStreamContent object that will be called when the output stream will be ready.
            result.Content = new PushStreamContent(async (outputStream, content, arg3) =>
            {
                try
                {
                    var dataDumper = new FilesystemDataDumper(FileSystem, smugglerOptions);
                    await dataDumper.ExportData(
                        new SmugglerExportOptions<FilesConnectionStringOptions>
                        {
                            ToStream = outputStream
                        }).ConfigureAwait(false);
                }
                finally
                {
                    outputStream.Close();
                }
            });

            var fileName = String.IsNullOrEmpty(smugglerOptions.NoneDefaultFileName) || (smugglerOptions.NoneDefaultFileName.IndexOfAny(Path.GetInvalidFileNameChars()) >= 0) ?
                string.Format("Dump of {0}, {1}", this.FileSystemName, DateTime.Now.ToString("yyyy-MM-dd HH-mm", CultureInfo.InvariantCulture)) :
                smugglerOptions.NoneDefaultFileName;
            result.Content.Headers.ContentDisposition = new ContentDispositionHeaderValue("attachment")
            {
                FileName = fileName + ".ravendump"
            };

            return new CompletedTask<HttpResponseMessage>(result);
        }

        private class ImportOperationStatus : IOperationState
        {
            public bool Completed { get; set; }
            public string LastProgress { get; set; }
            public string ExceptionDetails { get; set; }
            public bool Faulted { get; set; }
            public RavenJToken State { get; set; }
        }
    }
}
