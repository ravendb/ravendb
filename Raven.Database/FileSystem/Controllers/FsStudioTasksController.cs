// -----------------------------------------------------------------------
//  <copyright file="FsStudioTasksController.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Diagnostics;
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
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Json;
using Raven.Abstractions.Smuggler;
using Raven.Abstractions.Util;
using Raven.Database.FileSystem.Actions;
using Raven.Database.FileSystem.Bundles.Versioning.Plugins;
using Raven.Database.FileSystem.Smuggler;
using Raven.Database.Server.Controllers;
using Raven.Database.Server.WebApi.Attributes;
using Raven.Json.Linq;

namespace Raven.Database.FileSystem.Controllers
{
    public class FsStudioTasksController : BaseFileSystemApiController
    {
        [HttpGet]
        [RavenRoute("fs/{fileSystemName}/studio-tasks/check-sufficient-diskspace")]
        public async Task<HttpResponseMessage> CheckSufficientDiskspaceBeforeImport(long fileSize)
        {
            string tempRoot = Path.GetPathRoot(FileSystem.Configuration.TempPath);
            var rootPathToDriveInfo = new Dictionary<string, DriveInfo>();
            DriveInfo.GetDrives().ForEach(drive => rootPathToDriveInfo[drive.RootDirectory.FullName] = drive);
            DriveInfo tempFolderDrive;
            if (!rootPathToDriveInfo.TryGetValue(tempRoot, out tempFolderDrive) ||
                tempFolderDrive.AvailableFreeSpace - (long)(tempFolderDrive.TotalSize*0.1) < fileSize)
                throw new HttpResponseException(HttpStatusCode.BadRequest);

            return GetEmptyMessage();
        }

        [HttpPost]
        [RavenRoute("fs/{fileSystemName}/studio-tasks/import")]
        public async Task<HttpResponseMessage> ImportFilesystem(int batchSize, bool stripReplicationInformation, bool shouldDisableVersioningBundle)
        {
            if (!Request.Content.IsMimeMultipartContent())
            {
                throw new HttpResponseException(HttpStatusCode.UnsupportedMediaType);
            }

            string tempPath = FileSystem.Configuration.TempPath;
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

            var status = new DataDumperOperationStatus();
            var cts = new CancellationTokenSource();

            var task = Task.Run(async () =>
            {
                try
                {
                    var dataDumper = new FilesystemDataDumper(FileSystem);
                    dataDumper.Progress += s => status.MarkProgress(s);
                    var smugglerOptions = dataDumper.Options;
                    smugglerOptions.BatchSize = batchSize;
                    smugglerOptions.ShouldDisableVersioningBundle = shouldDisableVersioningBundle;
                    smugglerOptions.StripReplicationInformation = stripReplicationInformation;
                    smugglerOptions.CancelToken = cts;

                    await dataDumper.ImportData(new SmugglerImportOptions<FilesConnectionStringOptions> { FromFile = uploadedFilePath }).ConfigureAwait(false);
                }
                catch (Exception e)
                {
                    if (cts.Token.IsCancellationRequested)
                    {
                        status.MarkCanceled("Task was cancelled");
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
                    status.MarkFaulted(status.ExceptionDetails);
                    throw;
                }
                finally
                {
                    status.MarkCompleted();
                    File.Delete(uploadedFilePath);
                }
            }, cts.Token);

            long id;
            FileSystem.Tasks.AddTask(task, status, new TaskActions.PendingTaskDescription
            {
                StartTime = SystemTime.UtcNow,
                TaskType = TaskActions.PendingTaskType.ImportFileSystem,
                Payload = fileName
            }, out id, cts);

            return GetMessageWithObject(new
            {
                OperationId = id
            }, HttpStatusCode.Accepted);
        }

        [HttpPost]
        [RavenRoute("fs/{fileSystemName}/studio-tasks/exportFilesystem")]
        public Task<HttpResponseMessage> ExportFilesystem(StudioTasksController.ExportData smugglerOptionsJson)
        {
            var result = GetEmptyMessage();

            var taskId = smugglerOptionsJson.ProgressTaskId;
            var requestString = smugglerOptionsJson.DownloadOptions;
            SmugglerFilesOptions smugglerOptions;

            using (var jsonReader = new RavenJsonTextReader(new StringReader(requestString)))
            {
                var serializer = JsonExtensions.CreateDefaultJsonSerializer();
                smugglerOptions = (SmugglerFilesOptions) serializer.Deserialize(jsonReader, typeof (SmugglerFilesOptions));
            }

            var fileName = string.IsNullOrEmpty(smugglerOptions.NoneDefualtFileName) || (smugglerOptions.NoneDefualtFileName.IndexOfAny(Path.GetInvalidFileNameChars()) >= 0) ?
                $"Dump of {FileSystemName}, {DateTime.Now.ToString("yyyy-MM-dd HH-mm", CultureInfo.InvariantCulture)}" :
                smugglerOptions.NoneDefualtFileName;

            //create PushStreamContent object that will be called when the output stream will be ready.
            result.Content = new PushStreamContent(async (outputStream, content, arg3) =>
            {
                var status = new DataDumperOperationStatus();
                var tcs = new TaskCompletionSource<object>();
                var sp = Stopwatch.StartNew();

                try
                {
                    FileSystem.Tasks.AddTask(tcs.Task, status, new TaskActions.PendingTaskDescription
                    {
                        StartTime = SystemTime.UtcNow,
                        TaskType = TaskActions.PendingTaskType.ExportFileSystem,
                        Payload = "Exporting file system, file name: " + fileName
                    }, taskId, smugglerOptions.CancelToken, skipStatusCheck: true);

                    var dataDumper = new FilesystemDataDumper(FileSystem, smugglerOptions);
                    dataDumper.Progress += s => status.MarkProgress(s);
                    await dataDumper.ExportData(
                        new SmugglerExportOptions<FilesConnectionStringOptions>
                        {
                            ToStream = outputStream
                        }).ConfigureAwait(false);

                    const string message = "Completed export";
                    status.MarkCompleted(message, sp.Elapsed);
                }
                catch (OperationCanceledException e)
                {
                    status.MarkCanceled(e.Message);
                }
                catch (Exception e)
                {
                    status.ExceptionDetails = e.ToString();
                    status.MarkFaulted(e.ToString());

                    throw;
                }
                finally
                {
                    tcs.SetResult("Completed");
                    outputStream.Close();
                }
            });

            result.Content.Headers.ContentDisposition = new ContentDispositionHeaderValue("attachment")
            {
                FileName = fileName + ".ravenfsdump"
            };

            return new CompletedTask<HttpResponseMessage>(result);
        }

        [HttpGet]
        [RavenRoute("fs/{fileSystemName}/studio-tasks/next-operation-id")]
        public HttpResponseMessage GetNextTaskId()
        {
            var result = FileSystem.Tasks.GetNextTaskId();
            var response = Request.CreateResponse(HttpStatusCode.OK, result);
            return response;
        }

        private class DataDumperOperationStatus : OperationStateBase
        {
            public string ExceptionDetails { get; set; }
        }
    }
}
