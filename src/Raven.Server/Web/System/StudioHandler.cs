// -----------------------------------------------------------------------
//  <copyright file="Studio.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.IO.Compression;
using System.Net;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Hosting;
using Raven.Server.Routing;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using Microsoft.AspNetCore.Http;
using Raven.Client;
using Raven.Server.Commercial;
using Sparrow.Collections;
using Sparrow.Threading;
using Sparrow.Utils;
using Raven.Client.Extensions;

namespace Raven.Server.Web.System
{
    public class StudioHandler : RequestHandler
    {
        /// <summary>
        /// Control structure for a cached file
        /// </summary>
        private class CachedStaticFile
        {
            public string ContentType;
            public string ETag;
            public byte[] Contents;
            public byte[] CompressedContents;
        }

        /// <summary>
        /// Minimum number of pending entries to compress in order to start a cache processing task
        /// </summary>
        private const int PendingEntriesToProcess = 5;

        /// <summary>
        /// The actual cache
        /// </summary>
        private static readonly ConcurrentDictionary<string, Lazy<CachedStaticFile>> StaticContentCache = new ConcurrentDictionary<string, Lazy<CachedStaticFile>>(StringComparer.OrdinalIgnoreCase);

        /// <summary>
        /// Queue of files pending compression in the server
        /// </summary>
        private static readonly ConcurrentQueue<string> EntriesToCompress = new ConcurrentQueue<string>();

        /// <summary>
        /// Number of entries pending compression. Done this way to avoid queue fragmentation
        /// </summary>
        private static int _pendingEntriesToCompress;

        /// <summary>
        /// A flag that is raised only when the cache is unavailable for processing.
        /// </summary>
        private static readonly MultipleUseFlag CacheProcessingHappening = new MultipleUseFlag();

        private const int BufferSize = 16 * 1024;

        private static FileSystemWatcher _fileWatcher;
        private static FileSystemWatcher _zipFileWatcher;

        public const string ZipFileName = "Raven.Studio.zip";

        private static string _zipFilePath;
        private static long _zipFileLastChangeTicks;

        private const string ETagZipFileSource = "Z";
        private const string ETagFileSystemFileSource = "F";

        private static readonly ConcurrentSet<string> ZipFileEntries = new ConcurrentSet<string>(StringComparer.OrdinalIgnoreCase);
        private static readonly MultipleUseFlag ZipFileProcessingHappening = new MultipleUseFlag();
        private static readonly MultipleUseFlag ZipFileInitialized = new MultipleUseFlag();

        private static byte[] CompressFile(byte[] rawMemory, CompressionLevel level = CompressionLevel.Fastest)
        {
            using (var gZippedMemory = new MemoryStream())
            {
                using (var gZipStream = GetGzipStream(gZippedMemory, CompressionMode.Compress, level))
                {
                    gZipStream.Write(rawMemory, 0, rawMemory.Length);
                }

                return gZippedMemory.ToArray();
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static bool ShouldSkipCache(FileInfo file)
        {
            return FileExtensionToContentTypeMapping.ContainsKey(file.Extension) == false;
        }

        private static readonly string[] ExtensionsToSkipCompression = {
            ".png",
            ".jpg",
            ".gif",
            ".ico",
            ".svg",
            ".woff",
            ".woff2",
            ".ttf",
            ".appcache"
        };

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static bool ShouldSkipCompression(string filePath)
        {
            return ExtensionsToSkipCompression.Contains(Path.GetExtension(filePath));
        }

        private static readonly Dictionary<string, string> FileExtensionToContentTypeMapping = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            // HTML does not have charset because the HTML is expected to declare the charset itself
            {".html", "text/html"},
            {".htm", "text/html"},
            {".txt", "text/plain; charset=utf-8"},
            {".css", "text/css; charset=utf-8"},
            {".js", "text/javascript; charset=utf-8"},
            {".map", "text/javascript; charset=utf-8"},
            {".ico", "image/vnd.microsoft.icon"},
            {".jpg", "image/jpeg"},
            {".gif", "image/gif"},
            {".png", "image/png"},
            {".xap", "application/x-silverlight-2"},
            {".json", "application/json; charset=utf-8"},
            {".eot", "application/vnd.ms-fontobject"},
            {".svg", "image/svg+xml"},
            {".ttf", "application/octet-stream"},
            {".woff", "application/font-woff"},
            {".woff2", "application/font-woff2"},
            {".appcache", "text/cache-manifest; charset=utf-8"}
        };

        [RavenAction("/favicon.ico", "GET", AuthorizationStatus.UnauthenticatedClients)]
        public Task FavIcon()
        {
            return GetStudioFileInternal("favicon.ico");
        }

        [RavenAction("/auth-error.html", "GET", AuthorizationStatus.UnauthenticatedClients)]
        public Task StudioAuthError()
        {
            var error = GetStringQueryString("err");
            HttpContext.Response.Headers["Content-Type"] = "text/html; charset=utf-8";
            return HttpContext.Response.WriteAsync(HtmlUtil.RenderStudioAuthErrorPage(error));
        }

        [RavenAction("/eula/index.html", "GET", AuthorizationStatus.UnauthenticatedClients)]
        public Task GetEulaIndexFile()
        {
            if (ServerStore.LicenseManager.IsEulaAccepted)
            {
                // redirect to studio - if user didn't configured it yet
                // then studio endpoint redirect to wizard
                HttpContext.Response.Headers["Location"] = "/studio/index.html";
                HttpContext.Response.StatusCode = (int)HttpStatusCode.Moved;
                return Task.CompletedTask;
            }

            return GetStudioFileInternal("index.html");
        }

        [RavenAction("/eula/$", "GET", AuthorizationStatus.UnauthenticatedClients)]
        public Task GetEulaFile()
        {
            if (ServerStore.LicenseManager.IsEulaAccepted)
            {
                // redirect to studio - if user didn't configured it yet
                // then studio endpoint redirect to wizard
                HttpContext.Response.Headers["Location"] = "/studio/index.html";
                HttpContext.Response.StatusCode = (int)HttpStatusCode.Moved;
                return Task.CompletedTask;
            }
            string serverRelativeFileName = 
                RouteMatch.Url.Substring(
                    RouteMatch.MatchLength, 
                    RouteMatch.Url.Length - RouteMatch.MatchLength
            );
            return GetStudioFileInternal(serverRelativeFileName);
        }

        [RavenAction("/wizard/index.html", "GET", AuthorizationStatus.UnauthenticatedClients)]
        public Task GetSetupIndexFile()
        {
            if (ServerStore.LicenseManager.IsEulaAccepted == false)
            {
                // redirect to studio - if user didn't configured it yet
                // then studio endpoint redirect to wizard
                HttpContext.Response.Headers["Location"] = "/eula/index.html";
                HttpContext.Response.StatusCode = (int)HttpStatusCode.Moved;
                return Task.CompletedTask;
            }

            // if user asks for entry point but we are already configured redirect to studio
            if (ServerStore.Configuration.Core.SetupMode != SetupMode.Initial)
            {
                HttpContext.Response.Headers["Location"] = "/studio/index.html";
                HttpContext.Response.StatusCode = (int)HttpStatusCode.Moved;
                return Task.CompletedTask;
            }

            return GetStudioFileInternal("index.html");
        }

        [RavenAction("/wizard/$", "GET", AuthorizationStatus.UnauthenticatedClients)]
        public Task GetSetupFile()
        {
            var serverRelativeFileName = RouteMatch.Url.Substring(
                    RouteMatch.MatchLength, 
                    RouteMatch.Url.Length - RouteMatch.MatchLength
                );
            // if user asks for entry point but we are already configured redirect to studio
            if (ServerStore.Configuration.Core.SetupMode != SetupMode.Initial)
            {
                HttpContext.Response.Headers["Location"] = "/studio/" + serverRelativeFileName;
                HttpContext.Response.StatusCode = (int)HttpStatusCode.Moved;
                return Task.CompletedTask;
            }
            return GetStudioFileInternal(serverRelativeFileName);
        }

        [RavenAction("/studio/index.html", "GET", AuthorizationStatus.UnauthenticatedClients)]
        public Task GetStudioIndexFile()
        {
            if (ServerStore.LicenseManager.IsEulaAccepted == false)
            {
                HttpContext.Response.Headers["Location"] = "/eula/index.html";
                HttpContext.Response.StatusCode = (int)HttpStatusCode.Moved;
                return Task.CompletedTask;
            }

            // if user asks for entry point but we are NOT already configured redirect to setup
            if (ServerStore.Configuration.Core.SetupMode == SetupMode.Initial)
            {
                HttpContext.Response.Headers["Location"] = "/wizard/index.html";
                HttpContext.Response.StatusCode = (int)HttpStatusCode.TemporaryRedirect;
                return Task.CompletedTask;
            }

            return GetStudioFileInternal("index.html");
        }

        [RavenAction("/studio/$", "GET", AuthorizationStatus.UnauthenticatedClients)]
        public Task GetStudioFile()
        {
            string serverRelativeFileName = RouteMatch.Url.Substring(
                    RouteMatch.MatchLength, 
                    RouteMatch.Url.Length - RouteMatch.MatchLength
                );
            return GetStudioFileInternal(serverRelativeFileName);
        }

        private static Task _loadingFileToCache = null;

        private async Task GetStudioFileInternal(string serverRelativeFileName)
        {
            if (Server.Configuration.Http.UseResponseCompression)
            {
                // We may create a cache compression task if it is needed
                if (_pendingEntriesToCompress > PendingEntriesToProcess && CacheProcessingHappening.Raise())
                {
#pragma warning disable CS4014 // Because this call is not awaited, execution of the current method continues before the call is completed
                    Task.Run(() => CacheCompress()).ConfigureAwait(false);
#pragma warning restore CS4014 // Because this call is not awaited, execution of the current method continues before the call is completed
                }
            }

            HttpContext.Response.Headers["Raven-Static-Served-From"] = "Cache";
            if (await ServeFromCache(serverRelativeFileName))
                return;

            if (_loadingFileToCache == null)
            {
                await LoadFilesIntoCache();
            }

            HttpContext.Response.Headers["Raven-Static-Served-From"] = "Cache";
            if (await ServeFromCache(serverRelativeFileName))
                return;

            var env = (IHostingEnvironment)HttpContext.RequestServices.GetService(typeof(IHostingEnvironment));
            var basePath = Server.Configuration.Studio.Path ?? env.ContentRootPath;

            HttpContext.Response.Headers["Raven-Static-Served-From"] = "ZipFile";
            if (await ServeFromZipFile(basePath, serverRelativeFileName))
                return;

            // If nothing worked, just inform that the page was not found.
            HttpContext.Response.Headers["Raven-Static-Served-From"] = "Unserved";
            var message =
                "The following file was not available: " +
                $"{serverRelativeFileName}. Please make sure that the Raven" +
                ".Studio.zip file exist in the main directory (near the " +
                "Raven.Server.exe).";

            HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
            HttpContext.Response.Headers["Content-Type"] = "text/plain; charset=utf-8";

            await HttpContext.Response.WriteAsync(message);
        }

        private async Task LoadFilesIntoCache()
        {
            // we might need to load files to cache, but need to do so concurrently
            var tcs = new TaskCompletionSource<object>(TaskContinuationOptions.RunContinuationsAsynchronously);
            try
            {
                var runningTask = Interlocked.CompareExchange(ref _loadingFileToCache, tcs.Task, null) ?? tcs.Task;
                if (runningTask != tcs.Task)
                {
                    // we lost the race, let's wait for someone else
                    await runningTask;
                    return;
                }

                var env = (IHostingEnvironment)HttpContext.RequestServices.GetService(typeof(IHostingEnvironment));
                var basePath = Server.Configuration.Studio.Path ?? env.ContentRootPath;

                AddPathsToCache(basePath);

                tcs.TrySetResult(null);// done
            }
            catch (Exception e)
            {
                //if there is an error, and we own the task, clear it for the next request
                var _ = Interlocked.CompareExchange(ref _loadingFileToCache, null, tcs.Task);
                tcs.TrySetException(e);
                throw;
            }
            finally
            {
                tcs.TrySetCanceled();// if we 
            }
        }

        private void CacheCompress()
        {
            Debug.Assert(CacheProcessingHappening);

            // Avoid fragmenting the queue more than necessary
            var dispatch = EntriesToCompress.Count;

            // Notice that this is not always consistent. However, it is 
            // eventually consistent.
            Interlocked.Add(ref _pendingEntriesToCompress, -dispatch);

            for (; dispatch > 0; dispatch--)
            {
                if (!EntriesToCompress.TryDequeue(out var relativeServerFileName))
                    break;

                // The cache entry should already be there in the static cache
                // unless it has been removed by the pruning process, in which
                // case we just skip it.
                if (!StaticContentCache.TryGetValue(relativeServerFileName, out var uncompressedCachedStaticFile))
                    continue;

                // The cache may attempt to read compressed contents, so this
                // assignment has to happen atomically. It is also possible
                // that the file has been removed from the cache in between,
                // in which case the compression would be pointless. We can't
                // avoid such cases.
                Interlocked.Exchange(ref uncompressedCachedStaticFile.Value.CompressedContents,
                    CompressFile(uncompressedCachedStaticFile.Value.Contents, Server.Configuration.Http.StaticFilesResponseCompressionLevel));
            }

            CacheProcessingHappening.LowerOrDie();
        }

        private async Task<bool> ServeFromCache(string serverRelativeFileName)
        {
            if (StaticContentCache.TryGetValue(serverRelativeFileName, out var entry) == false)
                return false;

            var metadata = entry.Value;

            if (metadata.ETag == HttpContext.Request.Headers[Constants.Headers.IfNoneMatch])
            {
                HttpContext.Response.ContentType = metadata.ContentType;
                HttpContext.Response.StatusCode = (int)HttpStatusCode.NotModified;
                return true;
            }

            HttpContext.Response.ContentType = FileExtensionToContentTypeMapping[Path.GetExtension(serverRelativeFileName)];
            HttpContext.Response.Headers[Constants.Headers.Etag] = metadata.ETag;

            byte[] contentsToServe;
            if (ClientAcceptsGzipResponse() && metadata.CompressedContents != null)
            {
                HttpContext.Response.Headers[Constants.Headers.ContentEncoding] = "gzip";
                contentsToServe = metadata.CompressedContents;
            }
            else
            {
                contentsToServe = metadata.Contents;
            }

            // Set the content length accordingly
            HttpContext.Response.Headers[Constants.Headers.ContentLength] = contentsToServe.Length.ToString();
            await HttpContext.Response.Body.WriteAsync(contentsToServe, 0, contentsToServe.Length);

            return true;
        }

        private static CachedStaticFile BuildForCache(string cacheKey, string eTag, FileInfo fileInfo, Stream inputStream)
        {
            using (var staticFileContents = new MemoryStream())
            {
                inputStream.CopyTo(staticFileContents, BufferSize);

                var entry = new CachedStaticFile
                {
                    ContentType = FileExtensionToContentTypeMapping[fileInfo.Extension],
                    Contents = staticFileContents.ToArray(),
                    ETag = eTag
                };

                if (ShouldSkipCompression(fileInfo.FullName))
                    return entry;

                EntriesToCompress.Enqueue(cacheKey);
                Interlocked.Increment(ref _pendingEntriesToCompress);
                return entry;
            }
        }

        private void AddPathsToCache(string basePath)
        {
            foreach (string lookupPath in FileSystemLookupPaths)
            {
                var wwwRootBasePath = Path.GetFullPath(Path.Combine(basePath, lookupPath));
                string[] files;
                try
                {
                    files = Directory.GetFiles(wwwRootBasePath, "*.*", SearchOption.AllDirectories);
                }
                catch (Exception)
                {
                    // path not found, no access, etc
                    // this is expected because we are trying to
                    // find multiple directories, some of them are
                    // likely to not be there
                    continue;
                }


                foreach (var ext in FileExtensionToContentTypeMapping.Keys)
                {
                    foreach (var file in files)
                    {
                        if (file.EndsWith(ext, StringComparison.OrdinalIgnoreCase) == false)
                            continue;

                        var info = new FileInfo(file);
                        if (ShouldSkipCache(info))
                            continue;

                        StaticContentCache.TryAdd(file.Substring(wwwRootBasePath.Length + 1).Replace('\\', '/'), 
                            new Lazy<CachedStaticFile>(() =>
                           {
                               var serverRelativeFileName = file.Substring(wwwRootBasePath.Length);
                               var fileETag = GenerateETagFor(ETagFileSystemFileSource, serverRelativeFileName.GetHashCode(),
                                   info.LastWriteTimeUtc.Ticks);
                               using (var stream = info.OpenRead())
                               {
                                   return BuildForCache(serverRelativeFileName, fileETag, info, stream);
                               }
                           }));
                    }
                }

                try
                {
                    _fileWatcher = new FileSystemWatcher
                    {
                        Path = wwwRootBasePath,
                        Filter = "*.*"
                    };
                }
                catch (ArgumentException)
                {
                    // path does not exists or no permissions
                }
                if (_fileWatcher != null)
                    StartFileSystemWatcher(_fileWatcher, (_, __) => ClearCache(), (_, __) => ClearCache());


                break;
            }
        }

        private void ClearCache()
        {
            var tmp = _loadingFileToCache;
            // We clear this first because the process tolerates missing
            // content cache entries
            EntriesToCompress.Clear();
            StaticContentCache.Clear();

            // this will force us to rebuild the cache in the next request
            // we don't care too much about correctness here because we know
            // that this is dev mode only and we don't expect millisecond precision
            // from a dev hitting Ctrl+S in the IDE and F5 in the browser :-)
            Interlocked.CompareExchange(ref _loadingFileToCache, null, tmp);
        }


        private static readonly string[] FileSystemLookupPaths = {
            "src/Raven.Studio/wwwroot",
            "wwwroot",
            "../Raven.Studio/wwwroot",
            "../src/Raven.Studio/wwwroot",
            "../../../../src/Raven.Studio/wwwroot",
            "../../../../../src/Raven.Studio/wwwroot",
            "../../../../../../src/Raven.Studio/wwwroot"
        };

        private static void StartFileSystemWatcher(FileSystemWatcher watcher, FileSystemEventHandler createChangeDeleteHandler, RenamedEventHandler renameHandler)
        {
            watcher.IncludeSubdirectories = true;
            watcher.NotifyFilter = NotifyFilters.DirectoryName |
                           NotifyFilters.FileName | NotifyFilters.LastWrite | NotifyFilters.Size;

            watcher.Changed += createChangeDeleteHandler;
            watcher.Created += createChangeDeleteHandler;
            watcher.Deleted += createChangeDeleteHandler;
            watcher.Renamed += renameHandler;

            watcher.EnableRaisingEvents = true;
        }

        private bool ParseETag(out string hierarchyTag, out long fileVersion)
        {
            string clientETag = HttpContext.Request.Headers[Constants.Headers.IfNoneMatch];
            var clientETagParts = clientETag?.Split('@');

            if (clientETagParts?.Length != 3)
            {
                hierarchyTag = "Unknown";
                fileVersion = -1;
                return false;
            }

            hierarchyTag = clientETagParts[0];

            if (long.TryParse(clientETagParts[2], NumberStyles.Integer, null, out fileVersion) == false)
                return false;

            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private string GenerateETagFor(string hierarchyTag, int resourceId, long fileVersion = -1)
        {
            return $"{hierarchyTag}@{CharExtensions.ToInvariantString(resourceId)}@{CharExtensions.ToInvariantString(fileVersion)}";
        }

        private async Task<bool> ServeFromZipFile(string reportedBasePath, string serverRelativeFileName)
        {
            if (_zipFilePath == null)
            {
                var zipFilePath = Path.Combine(reportedBasePath, ZipFileName);
                if (File.Exists(zipFilePath) == false)
                    return false;

                // Many threads may find the path at once, only one stands
                if (Interlocked.CompareExchange(ref _zipFilePath, zipFilePath, null) == null)
                {
#pragma warning disable 4014
                    Task.Run(() => { InitializeZipFileServing(reportedBasePath); });
#pragma warning restore 4014
                }
            }

            // The ETag has to be different for every file in the Zip file,
            // and different across different versions of the file.
            if (ParseETag(out string eTagFileSource, out long eTagFileVersion) && eTagFileSource == ETagZipFileSource && eTagFileVersion == _zipFileLastChangeTicks)
            {
                HttpContext.Response.StatusCode = (int)HttpStatusCode.NotModified;
                return true;
            }

            // ETag answering failed. Check if we have the file in the current
            // version of the zip file. The index may be unavailable due to
            // recent changes.
            if (ZipFileProcessingHappening == false)
            {
                // The index is available, but we may not have started loading
                if (ZipFileEntries.Contains(serverRelativeFileName) == false && ZipFileInitialized)
                    return false;
            }

            // We have the file in the zip, so fetch it
            using (var fileStream = SafeFileStream.Create(_zipFilePath, FileMode.Open, FileAccess.Read, FileShare.Read))
            using (var zipArchive = new ZipArchive(fileStream, ZipArchiveMode.Read, false))
            {
                var zipEntry = zipArchive.Entries.FirstOrDefault(a => a.FullName.Equals(serverRelativeFileName, StringComparison.OrdinalIgnoreCase));
                if (zipEntry == null)
                    return false;

                // This does not represent an actual file, just the metadata
                // in the file name
                var fileInfo = new FileInfo(serverRelativeFileName);
                string fileETag = GenerateETagFor(ETagZipFileSource, serverRelativeFileName.GetHashCode(), _zipFileLastChangeTicks);

                HttpContext.Response.ContentType = FileExtensionToContentTypeMapping[fileInfo.Extension];
                HttpContext.Response.Headers[Constants.Headers.Etag] = fileETag;

                using (var entry = zipEntry.Open())
                using (var responseStream = ResponseBodyStream())
                {
                    await entry.CopyToAsync(responseStream, BufferSize);
                }

                // The zip entry stream is not seekable, so we have to reopen it
                using (var entry = zipEntry.Open())
                {
                    var cacheEntry = BuildForCache(serverRelativeFileName, fileETag, fileInfo, entry);
                    StaticContentCache.TryAdd(serverRelativeFileName, new Lazy<CachedStaticFile>(cacheEntry));
                }
            }

            return true;
        }

        private static void InitializeZipFileServing(string reportedBasePath)
        {
            Debug.Assert(_zipFilePath != null);

            // We need to invalidate the cache if the zip file changes
            _zipFileWatcher = new FileSystemWatcher();
            _zipFileWatcher.Path = reportedBasePath;
            _zipFileWatcher.Filter = ZipFileName;
            StartFileSystemWatcher(_zipFileWatcher, ZipFileChangedEventHandler, ZipFileChangedEventHandler);
            ReprocessZipFile();
        }

        private static void ZipFileChangedEventHandler(object sender, FileSystemEventArgs e)
        {
            // Do not change the order of these instructions. We want to avoid 
            // polluting the ETags in the new cache with old ones.
            Interlocked.Increment(ref _zipFileLastChangeTicks);
            StaticContentCache.Clear();
            ReprocessZipFile();
        }

        private static void ReprocessZipFile()
        {
            Debug.Assert(_zipFilePath != null);

            // Make sure to keep the relative order between the flags,  this
            // code runs in multiple threads
            if (ZipFileProcessingHappening.Raise() == false)
                return;

            ZipFileInitialized.Lower();
            ZipFileEntries.Clear();

            try
            {
                using (var fileStream = SafeFileStream.Create(_zipFilePath, FileMode.Open, FileAccess.Read, FileShare.Read))
                using (var zipArchive = new ZipArchive(fileStream, ZipArchiveMode.Read, false))
                {
                    foreach (var entry in zipArchive.Entries)
                    {
                        ZipFileEntries.Add(entry.FullName);
                    }
                }
            }
            catch (Exception)
            {
                // Suppressing this exception is reasonable: there are many 
                // reasons for which the file may not be available right now.
                // The watcher will let us know whenever we can try again.
            }

            ZipFileInitialized.RaiseOrDie();
            ZipFileProcessingHappening.LowerOrDie();
        }

        [RavenAction("/", "GET", AuthorizationStatus.UnauthenticatedClients)]
        public Task RavenRoot()
        {
            HttpContext.Response.Headers["Location"] = "/studio/index.html";
            HttpContext.Response.StatusCode = (int)HttpStatusCode.MovedPermanently;

            return Task.CompletedTask;
        }
    }
}
