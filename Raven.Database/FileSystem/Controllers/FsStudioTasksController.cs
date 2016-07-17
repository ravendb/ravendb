// -----------------------------------------------------------------------
//  <copyright file="FsStudioTasksController.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
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

            var status = new ImportOperationStatus();
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
                Payload = fileName,

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

            try
            {
                var requestString = smugglerOptionsJson.SmugglerOptions;
                SmugglerFilesOptions smugglerOptions;

                using (var jsonReader = new RavenJsonTextReader(new StringReader(requestString)))
                {
                    var serializer = JsonExtensions.CreateDefaultJsonSerializer();
                    smugglerOptions = (SmugglerFilesOptions)serializer.Deserialize(jsonReader, typeof(SmugglerFilesOptions));
                }

                //create PushStreamContent object that will be called when the output stream will be ready.
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

                var fileName = string.IsNullOrEmpty(smugglerOptions.NoneDefualtFileName) || (smugglerOptions.NoneDefualtFileName.IndexOfAny(Path.GetInvalidFileNameChars()) >= 0) ?
                    $"Dump of {FileSystemName}, {DateTime.Now.ToString("yyyy-MM-dd HH-mm", CultureInfo.InvariantCulture)}" :
                    smugglerOptions.NoneDefualtFileName;
                result.Content.Headers.ContentDisposition = new ContentDispositionHeaderValue("attachment")
                {
                    FileName = fileName + ".ravenfsdump"
                };
            }
            catch (Exception e)
            {
                result.StatusCode = HttpStatusCode.InternalServerError;
                result.Content = new StringContent(e.Message);
            }

            return new CompletedTask<HttpResponseMessage>(result);
        }

        private class ImportOperationStatus : OperationStateBase
        {
            public string ExceptionDetails { get; set; }
        }
    }
}
