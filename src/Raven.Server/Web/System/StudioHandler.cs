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
using StringSegment = Sparrow.StringSegment;

namespace Raven.Server.Web.System
{
    public class StudioHandler : RequestHandler
    {
        /// <summary>
        /// Control structure for a cached file
        /// </summary>
        class CachedStaticFile
        {
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
        private static readonly ConcurrentDictionary<string, CachedStaticFile> StaticContentCache = new ConcurrentDictionary<string, CachedStaticFile>(StringComparer.OrdinalIgnoreCase);

        /// <summary>
        /// Queue of files pending compression in the server
        /// </summary>
        private static readonly ConcurrentQueue<string> EntriesToCompress = new ConcurrentQueue<string>();

        /// <summary>
        /// Number of entries pending compression. Done this way to avoid queue fragmentation
        /// </summary>
        private static int _pendingEntriesToCompress = 0;

        /// <summary>
        /// A flag that is raised only when the cache is unavailable for processing.
        /// </summary>
        private static readonly MultipleUseFlag CacheProcessingHappening = new MultipleUseFlag();

        private const int BufferSize = 16 * 1024;

        private static string _wwwRootBasePath;

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
        private static bool ShouldSkipCache(FileInfo fileInfo)
        {
            return false;
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
            {".css", "text/css; charset=utf-8"},
            {".js", "text/javascript; charset=utf-8"},
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

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static string GetContentType(string fileExtension)
        {
            return FileExtensionToContentTypeMapping.TryGetValue(fileExtension, out string contentType) ?
                contentType :
                "text/plain; charset=utf-8";
        }

        [RavenAction("/favicon.ico", "GET", AuthorizationStatus.UnauthenticatedClients)]
        public Task FavIcon()
        {
            HttpContext.Response.StatusCode = 404;
            return Task.CompletedTask;
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
            string serverRelativeFileName = new StringSegment(
                RouteMatch.Url, RouteMatch.MatchLength, RouteMatch.Url.Length - RouteMatch.MatchLength);
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
            string serverRelativeFileName = new StringSegment(
                RouteMatch.Url, RouteMatch.MatchLength, RouteMatch.Url.Length - RouteMatch.MatchLength);
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
            // This is casted to string on purpose here. Everything else works
            // with strings, so reifying this now is good.
            string serverRelativeFileName = new StringSegment(
                RouteMatch.Url, RouteMatch.MatchLength, RouteMatch.Url.Length - RouteMatch.MatchLength);
            return GetStudioFileInternal(serverRelativeFileName);
        }
        
        private async Task GetStudioFileInternal(string serverRelativeFileName)
        {
            HttpContext.Response.Headers["Raven-Static-Served-From"] = "Cache";
            if (await ServeFromCache(serverRelativeFileName))
                return;

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

            var env = (IHostingEnvironment)HttpContext.RequestServices.GetService(typeof(IHostingEnvironment));
            var basePath = Server.Configuration.Studio.Path ?? env.ContentRootPath;

            HttpContext.Response.Headers["Raven-Static-Served-From"] = "ZipFile";
            if (await ServeFromZipFile(basePath, serverRelativeFileName))
                return;

            HttpContext.Response.Headers["Raven-Static-Served-From"] = "FileSystem";
            if (await ServeFromFileSystem(basePath, serverRelativeFileName))
                return;

            // If nothing worked, just inform that the page was not found.
            HttpContext.Response.Headers["Raven-Static-Served-From"] = "Unserved";
            var message =
                $"The following file was not available: " +
                $"{serverRelativeFileName}. Please make sure that the Raven" +
                $".Studio.zip file exist in the main directory (near the " +
                $"Raven.Server.exe).";
            
            HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
            HttpContext.Response.Headers["Content-Type"] = "text/plain; charset=utf-8";
            
            await HttpContext.Response.WriteAsync(message);
        }

        private void CacheCompress()
        {
            Debug.Assert(CacheProcessingHappening);

            // Avoid fragmenting the queue more than necessary
            int dispatch = EntriesToCompress.Count;

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
                Interlocked.Exchange(ref uncompressedCachedStaticFile.CompressedContents,
                    CompressFile(uncompressedCachedStaticFile.Contents, Server.Configuration.Http.StaticFilesResponseCompressionLevel));
            }

            CacheProcessingHappening.LowerOrDie();
        }

        private async Task<bool> ServeFromCache(string serverRelativeFileName)
        {

            if (StaticContentCache.TryGetValue(serverRelativeFileName, out var metadata) == false)
                return false;

            if (metadata.ETag == HttpContext.Request.Headers[Constants.Headers.IfNoneMatch])
            {
                HttpContext.Response.StatusCode = (int)HttpStatusCode.NotModified;
                return true;
            }

            HttpContext.Response.ContentType = GetContentType(Path.GetExtension(serverRelativeFileName));
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

        /// <summary>
        /// Retrieves a file from the filesystem and serves it to a client.
        /// 
        /// This should only be called when the cache can not find the file,
        /// or when it is disabled.
        /// </summary>
        private async Task<bool> ServeFromFileSystem(string reportedBasePath, string serverRelativeFileName)
        {
            FileInfo staticFileInfo;
            if (_wwwRootBasePath != null)
                staticFileInfo = new FileInfo(Path.Combine(_wwwRootBasePath, serverRelativeFileName));
            else
                staticFileInfo = FindRootBasePath(reportedBasePath, serverRelativeFileName);

            if (staticFileInfo == null || staticFileInfo.Exists == false)
                return false;

            var fileETag = GenerateETagFor(ETagFileSystemFileSource, serverRelativeFileName.GetHashCode(), staticFileInfo.LastWriteTimeUtc.Ticks);
            if (fileETag == HttpContext.Request.Headers[Constants.Headers.IfNoneMatch])
            {
                HttpContext.Response.StatusCode = (int)HttpStatusCode.NotModified;
                return true;
            }

            HttpContext.Response.ContentType = GetContentType(staticFileInfo.Extension);
            HttpContext.Response.Headers[Constants.Headers.Etag] = fileETag;

            using (var handle = File.OpenRead(staticFileInfo.FullName))
            {
                using (var responseStream = ResponseBodyStream())
                    await handle.CopyToAsync(responseStream, BufferSize);

                handle.Seek(0, SeekOrigin.Begin);

                await AddToCache(serverRelativeFileName, fileETag, staticFileInfo, handle);
            }

            return true;
        }

        private async Task AddToCache(string cacheKey, string eTag, FileInfo fileInfo, Stream inputStream)
        {
            if (ShouldSkipCache(fileInfo))
                return;

            using (var staticFileContents = new MemoryStream())
            {
                await inputStream.CopyToAsync(staticFileContents, BufferSize);

                StaticContentCache.TryAdd(cacheKey, new CachedStaticFile
                {
                    Contents = staticFileContents.ToArray(),
                    ETag = eTag
                });

                if (ShouldSkipCompression(fileInfo.FullName))
                    return;

                EntriesToCompress.Enqueue(cacheKey);
                Interlocked.Increment(ref _pendingEntriesToCompress);
            }
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

        private FileInfo FindRootBasePath(string reportedBasePath, string serverRelativeFileName)
        {
            Debug.Assert(FileSystemLookupPaths.Length > 0);

            FileInfo staticFileInfo = null;
            string wwwRootBasePath = null;
            bool fileWasFound = false;
            
            foreach (string lookupPath in FileSystemLookupPaths)
            {
                wwwRootBasePath = Path.Combine(reportedBasePath, lookupPath);
                staticFileInfo = new FileInfo(Path.Combine(wwwRootBasePath, serverRelativeFileName));

                if (staticFileInfo.Exists)
                {
                    fileWasFound = true;
                    break;
                }
            }
            
            // prevent from using last path when resource wasn't found
            if (fileWasFound == false)
            {
                return null;
            }

            // Many threads may find the path at once, only one stands
            wwwRootBasePath = Path.GetFullPath(wwwRootBasePath);
            if (Interlocked.CompareExchange(ref _wwwRootBasePath, wwwRootBasePath, null) == null)
            {
                // When the thread that finds the root path (the first request
                // ever to be handled by the server) changes it, it also
                // starts the process to check if files change and invalidate
                // cache entries.
                try
                {
                    _fileWatcher = new FileSystemWatcher
                    {
                        Path = _wwwRootBasePath,
                        Filter = "*.*"
                    };
                }
                catch (ArgumentException)
                {
                    // path does not exists or no permissions
                }
                if (_fileWatcher != null)
                    StartFileSystemWatcher(_fileWatcher, CacheEvictEntryEventHandler, CacheEvictRenamedEntryEventHandler);

            }

            return staticFileInfo;
        }

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

        private void CacheEvictEntryEventHandler(object sender, FileSystemEventArgs e)
        {
            if (File.Exists(e.FullPath) || Directory.Exists(e.FullPath) == false)
            {
                // It is a file, or it does not exist any more
                string relativePath = Path.GetRelativePath(_wwwRootBasePath, e.FullPath).Replace('\\', '/');
                StaticContentCache.TryRemove(relativePath, out var value);
            }
            else
            {
                // It is not a file, clear the cache
                ClearCache();
            }
        }

        private void ClearCache()
        {
            // We clear this first because the process tolerates missing
            // content cache entries
            EntriesToCompress.Clear();
            StaticContentCache.Clear();
        }

        private void CacheEvictRenamedEntryEventHandler(object sender, RenamedEventArgs e)
        {
            if (File.Exists(e.FullPath) || Directory.Exists(e.FullPath) == false)
            {
                // It is a file, or it does not exist any more. Notice we
                // clear the old version.
                string relativePath = Path.GetRelativePath(_wwwRootBasePath, e.OldFullPath).Replace('\\', '/');
                StaticContentCache.TryRemove(relativePath, out var value);
            }
            else
            {
                // It is not a file, clear the cache
                ClearCache();
            }
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
            return $"{hierarchyTag}@{resourceId}@{fileVersion}";
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

                HttpContext.Response.ContentType = GetContentType(fileInfo.Extension);
                HttpContext.Response.Headers[Constants.Headers.Etag] = fileETag;

                using (var entry = zipEntry.Open())
                using (var responseStream = ResponseBodyStream())
                {
                    await entry.CopyToAsync(responseStream, BufferSize);
                }

                // The zip entry stream is not seekable, so we have to reopen it
                using (var entry = zipEntry.Open())
                    await AddToCache(serverRelativeFileName, fileETag, fileInfo, entry);
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
                // Supressing this exception is reasonable: there are many 
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
