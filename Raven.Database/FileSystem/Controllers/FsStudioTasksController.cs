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
using Raven.Abstractions.Database.Smuggler;
using Raven.Abstractions.Database.Smuggler.FileSystem;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Json;
using Raven.Abstractions.Util;
using Raven.Database.FileSystem.Actions;
using Raven.Database.FileSystem.Bundles.Versioning.Plugins;
using Raven.Database.FileSystem.Smuggler;
using Raven.Database.FileSystem.Smuggler.Embedded;
using Raven.Database.Server.Controllers;
using Raven.Database.Server.WebApi.Attributes;
using Raven.Json.Linq;
using Raven.Smuggler.FileSystem;
using Raven.Smuggler.FileSystem.Streams;

namespace Raven.Database.FileSystem.Controllers
{
    public class FsStudioTasksController : BaseFileSystemApiController
    {
        [HttpPost]
        [RavenRoute("fs/{fileSystemName}/studio-tasks/import")]
        public async Task<HttpResponseMessage> ImportFilesystem(int batchSize, bool stripReplicationInformation, bool shouldDisableVersioningBundle)
        {
            if (!Request.Content.IsMimeMultipartContent())
            {
                throw new HttpResponseException(HttpStatusCode.UnsupportedMediaType);
            }

            string tempPath = FileSystem.Configuration.Core.TempPath;
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
                    using (var fileStream = File.Open(uploadedFilePath, FileMode.Open, FileAccess.Read))
                    {
                        var options = new FileSystemSmugglerOptions
                                      {
                                          BatchSize = batchSize,
                                          StripReplicationInformation = stripReplicationInformation,
                                          ShouldDisableVersioningBundle = shouldDisableVersioningBundle
                                      };

                        var smuggler = new FileSystemSmuggler(options);

                        smuggler.Notifications.OnProgress += (sender, message) => status.LastProgress = message;
                        await smuggler.ExecuteAsync(new StreamSmugglingSource(fileStream), new EmbeddedSmugglingDestination(FileSystem), cts.Token).ConfigureAwait(false);
                    }
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
                    else if (e is OperationVetoedException && e.Message.Contains(VersioningTriggerActions.CreationOfHistoricalRevisionIsNotAllowed))
                    {
                        status.ExceptionDetails = "You are trying to import historical documents while the versioning bundle is enabled. " +
                                                  "You should disable versioning during such import. Please mark the checkbox 'Disable versioning bundle during import' at Import File System";
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
            }, HttpStatusCode.Accepted);
        }

        [HttpPost]
        [RavenRoute("fs/{fileSystemName}/studio-tasks/exportFilesystem")]
        public Task<HttpResponseMessage> ExportFilesystem(StudioTasksController.ExportData exportData)
        {
            var requestString = exportData.SmugglerOptions;
            FileSystemSmugglerOptions options;
            
            using (var jsonReader = new RavenJsonTextReader(new StringReader(requestString)))
            {
                var serializer = JsonExtensions.CreateDefaultJsonSerializer();
                options = (FileSystemSmugglerOptions)serializer.Deserialize(jsonReader, typeof(FileSystemSmugglerOptions));
            }


            var result = GetEmptyMessage();

            // create PushStreamContent object that will be called when the output stream will be ready.
            result.Content = new PushStreamContent((outputStream, content, arg3) =>
            {
                try
                {
                    var smuggler = new FileSystemSmuggler(options);

                    return smuggler.ExecuteAsync(new EmbeddedSmugglingSource(FileSystem), new StreamSmugglingDestination(outputStream, leaveOpen: true));
                }
                finally
                {
                    outputStream.Close();
                }
            });

            var fileName = string.IsNullOrEmpty(exportData.FileName) || (exportData.FileName.IndexOfAny(Path.GetInvalidFileNameChars()) >= 0) ?
                string.Format("Dump of {0}, {1}", FileSystemName, DateTime.Now.ToString("yyyy-MM-dd HH-mm", CultureInfo.InvariantCulture)) :
                exportData.FileName;

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
