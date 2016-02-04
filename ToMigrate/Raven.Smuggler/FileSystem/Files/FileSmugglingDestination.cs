// -----------------------------------------------------------------------
//  <copyright file="FileSmugglingDestination.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Globalization;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Database.Smuggler.FileSystem;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Util;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;
using Raven.Smuggler.FileSystem.Streams;

namespace Raven.Smuggler.FileSystem.Files
{
    public class FileSmugglingDestination : IFileSystemSmugglerDestination
    {
        private const string IncrementalExportStateFile = "IncrementalExport.state.json";

        private string path;

        private readonly bool incremental;

        private StreamSmugglingDestination streamDestination;

        public FileSmugglingDestination(string fileOrDirectoryPath, bool incremental)
        {
            path = Path.GetFullPath(fileOrDirectoryPath);
            this.incremental = incremental;
        }

        public void Dispose()
        {
            streamDestination?.Dispose();
        }

        public Task InitializeAsync(FileSystemSmugglerOptions options, FileSystemSmugglerNotifications notifications, CancellationToken cancellationToken)
        {
            if (incremental)
            {
                if (Directory.Exists(path) == false)
                {
                    if (File.Exists(path))
                        path = Path.GetDirectoryName(path) ?? path;
                    else
                        Directory.CreateDirectory(path);
                }

                if (options.StartFilesEtag == Etag.Empty)
                {
                    var lastExportedEtags = ReadLastEtagsFromIncrementalExportFile(path);

                    if (lastExportedEtags != null)
                    {
                        options.StartFilesEtag = lastExportedEtags.LastFileEtag;
                        options.StartFilesDeletionEtag = lastExportedEtags.LastDeletedFileEtag;
                    }
                }

                path = Path.Combine(path, SystemTime.UtcNow.ToString("yyyy-MM-dd-HH-mm-0", CultureInfo.InvariantCulture) + ".ravenfs-incremental-dump");

                if (File.Exists(path))
                {
                    var counter = 1;
                    while (true)
                    {
                        path = Path.Combine(Path.GetDirectoryName(path), SystemTime.UtcNow.ToString("yyyy-MM-dd-HH-mm", CultureInfo.InvariantCulture) + "-" + counter + ".ravenfs-incremental-dump");

                        if (File.Exists(path) == false)
                            break;

                        counter++;
                    }
                }
            }

            streamDestination = new StreamSmugglingDestination(File.Create(path));

            return streamDestination.InitializeAsync(options, notifications, cancellationToken);
        }

        public ISmuggleFilesToDestination WriteFiles()
        {
            return streamDestination.WriteFiles();
        }

        public ISmuggleConfigurationsToDestination WriteConfigurations()
        {
            return streamDestination.WriteConfigurations();
        }

        public Task AfterExecuteAsync(FileSystemSmugglerOperationState state)
        {
            if (incremental)
                WriteLastEtagsToFile(state, Path.GetDirectoryName(path));

            state.OutputPath = path;

            return new CompletedTask();
        }

        public void OnException(SmugglerException exception)
        {
            exception.File = path;
        }

        private static LastFilesEtagsInfo ReadLastEtagsFromIncrementalExportFile(string path)
        {
            var etagFileLocation = Path.Combine(path, IncrementalExportStateFile);
            if (!File.Exists(etagFileLocation))
                return null;

            using (var streamReader = new StreamReader(new FileStream(etagFileLocation, FileMode.Open)))
            using (var jsonReader = new JsonTextReader(streamReader))
            {
                RavenJObject ravenJObject;
                try
                {
                    ravenJObject = RavenJObject.Load(jsonReader);
                }
                catch (Exception e)
                {
                    //TODO arek log.WarnException("Could not parse etag document from file : " + etagFileLocation + ", ignoring, will start from scratch", e);
                    return null;
                }

                var result = new LastFilesEtagsInfo
                {
                    LastFileEtag = Etag.Parse(ravenJObject.Value<string>("LastFileEtag")),
                    LastDeletedFileEtag = Etag.Parse(ravenJObject.Value<string>("LastDeletedFileEtag") ?? Etag.Empty.ToString())
                };

                return result;
            }
        }

        public static void WriteLastEtagsToFile(FileSystemSmugglerOperationState state, string backupPath)
        {
            var etagFileLocation = Path.Combine(backupPath, IncrementalExportStateFile);
            using (var streamWriter = new StreamWriter(File.Create(etagFileLocation)))
            {
                new RavenJObject
                    {
                        {"LastFileEtag", state.LastFileEtag.ToString()},
                        {"LastDeletedFileEtag", state.LastDeletedFileEtag.ToString()},
                    }.WriteTo(new JsonTextWriter(streamWriter));
            }
        }
    }
}