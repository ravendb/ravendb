// -----------------------------------------------------------------------
//  <copyright file="Studio.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Net;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Hosting;
using Raven.Server.Routing;
using System.Linq;
using System.Net.Http.Headers;
using Microsoft.Extensions.Primitives;
using Raven.Client;
using Raven.Client.Extensions.Streams;
using Raven.Client.Util;
using StringSegment = Sparrow.StringSegment;

namespace Raven.Server.Web.System
{
    public class StudioHandler : RequestHandler
    {
        private static readonly ConcurrentDictionary<string, long> ZipLastChangedDate = new ConcurrentDictionary<string, long>(StringComparer.OrdinalIgnoreCase);

        private static DateTime _lastFileNamesUpdate = DateTime.MinValue;

        private static Dictionary<string, FileInfo> _fileNamesCaseInsensitive = new Dictionary<string, FileInfo>(StringComparer.OrdinalIgnoreCase);

        public static readonly string[] LookupPaths = {
            "src/Raven.Studio/wwwroot",
             "wwwroot",
            "../Raven.Studio/wwwroot",
            "../src/Raven.Studio/wwwroot",
            "../../../../src/Raven.Studio/wwwroot",
            "../../../../../src/Raven.Studio/wwwroot",
            "../../../../../../src/Raven.Studio/wwwroot"
        };

        public static readonly Dictionary<string, string> FileExtensionToContentTypeMapping = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
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

        private static FileInfo TryGetFileName(string basePath, string fileName)
        {
            // this is expected to run concurrently
            if (_fileNamesCaseInsensitive.TryGetValue(fileName, out FileInfo value))
                return value;

            if ((SystemTime.UtcNow - _lastFileNamesUpdate).TotalSeconds < 3)
                return null;

            var files = new Dictionary<string, FileInfo>(StringComparer.OrdinalIgnoreCase);
            foreach (var lookupPathDir in LookupPaths)
            {
                var lookupPath = Path.Combine(basePath, lookupPathDir);
                if (Directory.Exists(lookupPath) == false)
                {
                    lookupPath = Path.Combine(Directory.GetCurrentDirectory(), lookupPathDir);
                    if (Directory.Exists(lookupPath) == false)
                        continue;
                }

                foreach (var file in Directory.GetFiles(lookupPath, "*", SearchOption.AllDirectories))
                {
                    files[file.Substring(lookupPath.Length + 1).Replace("\\", "/")] = new FileInfo(file);
                }
            }

            _lastFileNamesUpdate = DateTime.UtcNow;
            if (files.TryGetValue(fileName, out value))
            {
                // only replace the value if there is a point to it, note that concurrent threads
                // may create it multiple times, until it is settled
                _fileNamesCaseInsensitive = files;
                return value;
            }
            return null;
        }


        // This is used to serve clients that don't support gzip
        private static readonly ConcurrentDictionary<string, byte[]> StaticContentCache = new ConcurrentDictionary<string, byte[]>();
        private static readonly ConcurrentDictionary<string, byte[]> CompressedStaticContentCache = new ConcurrentDictionary<string, byte[]>();

        public async Task WriteFile(FileInfo file)
        {
#if DEBUG
            HttpContext.Response.Headers["File-Path"] = file.FullName;
#endif
            var etagValue = HttpContext.Request.Headers["If-None-Match"];

            var fileEtag = '"' + file.LastWriteTimeUtc.ToString("G") + '"';
            if (etagValue == fileEtag)
            {
                HttpContext.Response.StatusCode = (int)HttpStatusCode.NotModified; // Not Modified
                return;
            }

            HttpContext.Response.ContentType = GetContentType(file.Extension);
            HttpContext.Response.Headers["ETag"] = fileEtag;


            // Load the uncompressed file, either from cache or disk
            string fileCacheKey = fileEtag + file.FullName;

            byte[] inputFile = StaticContentCache.GetOrAdd(fileCacheKey, _ => File.ReadAllBytesAsync(file.FullName).Result);
            Stream inputStream = new MemoryStream(inputFile);

            // Transfer the file all at once, these are not actual streams, so 
            // there is no need to chunk
            HttpContext.Response.Headers["Transfer-Encoding"] = "identity";
            if (ShouldSkipCompression(file) || !AcceptsGzipResponse())
            {
                // Serve from _fileCache
                HttpContext.Response.Headers["Content-Length"] = inputFile.Length.ToString();
            }
            else
            {
                // Serve from _compressedFileCache using gzip encoding
                HttpContext.Response.Headers["Content-Encoding"] = "gzip";
                var gzippedFile = CompressedStaticContentCache.GetOrAdd(fileCacheKey, _ =>
                    {
                        var responseStream = new MemoryStream();
                        // Gzip the inputFileStream and put it into the responseStream. The stream is flushed on Dispose
                        using (var gZipStream = GetGzipStream(responseStream, CompressionMode.Compress, CompressionLevel.Optimal))
                        {
                            // ReSharper disable once AccessToModifiedClosure
                            inputStream.CopyTo(gZipStream);
                        }

                        // Convert the responseStream into an array
                        return responseStream.ToArray();
                    });

                HttpContext.Response.Headers["Content-Length"] = gzippedFile.Length.ToString();
                inputStream = new MemoryStream(gzippedFile);
            }


            // Since we are copying from a MemoryStream, buffering only hurts
            var responseBodyStream = HttpContext.Response.Body;
            await inputStream
                    .CopyToAsync(responseBodyStream)
                    .ContinueWith(task => responseBodyStream.FlushAsync());
        }

        private static readonly List<string> SkipExtensions = new List<string>
            {
                ".png",
                ".jpg",
                ".ico",
                ".svg",
                ".woff2",
                ".ttf"
            };

        private static bool ShouldSkipCompression(FileInfo file)
        {
            return SkipExtensions.Contains(file.Extension);
        }

        [RavenAction("/studio/$", "GET", AuthorizationStatus.UnauthenticatedClients)]
        public async Task GetStudioFile()
        {
            var fileName = new StringSegment(
                RouteMatch.Url, RouteMatch.MatchLength, RouteMatch.Url.Length - RouteMatch.MatchLength);

            var env = (IHostingEnvironment)HttpContext.RequestServices.GetService(typeof(IHostingEnvironment));

            var basePath = Server.Configuration.Studio.Path ?? env.ContentRootPath;

            var file = TryGetFileName(basePath, fileName);
            if (file != null)
            {
                await WriteFile(file);
                return;
            }

            var zipFilePath = Path.Combine(basePath, "Raven.Studio.zip");

            if (File.Exists(zipFilePath) == false)
            {
                HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                return;
            }

            WriteFileFromZip(zipFilePath, fileName);
        }

        [RavenAction("/", "GET", AuthorizationStatus.UnauthenticatedClients)]
        public Task RavenRoot()
        {
            HttpContext.Response.Headers["Location"] = "/studio/index.html";
            HttpContext.Response.StatusCode = (int)HttpStatusCode.MovedPermanently;
            return Task.CompletedTask;
        }

        private void WriteFileFromZip(string zipPath, string fileName)
        {
            var etagValue = GetLongFromHeaders("If-None-Match") ?? 
                GetLongFromHeaders("If-Match");
            var currentFileEtag = ZipLastChangedDate.GetOrAdd(
                zipPath, 
                f => File.GetLastWriteTime(f).Ticks);

            if (etagValue == currentFileEtag)
            {
                HttpContext.Response.StatusCode = (int)HttpStatusCode.NotModified;
                return;
            }

            var fileStream = new FileStream(zipPath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
            var zipArchive = new ZipArchive(fileStream, ZipArchiveMode.Read, false);

            var zipEntry = zipArchive.Entries.FirstOrDefault(a => a.FullName.Equals(fileName, StringComparison.OrdinalIgnoreCase));
            if (zipEntry == null)
            {
                WriteEmbeddedFileNotFound(fileName);
                return;
            }

            using (var responseStream = ResponseBodyStream())
            {
                var file = new FileInfo(fileName);
                var type = GetContentType(file.Extension);
                HttpContext.Response.ContentType = new MediaTypeHeaderValue(type).ToString();
                WriteETag(currentFileEtag);
                using (var entry = zipEntry.Open())
                {
                    entry.CopyTo(responseStream);
                    entry.Flush();
                }
            }
        }

        private static string GetContentType(string fileExtension)
        {
            return FileExtensionToContentTypeMapping.TryGetValue(fileExtension, out string contentType) ? contentType : "text/plain; charset=utf-8";
        }

        public string GetHeader(string key)
        {
            var requestHeaders = HttpContext.Request.Headers;
            if (requestHeaders.TryGetValue(key, out StringValues values))
                return values.FirstOrDefault();
            return null;
        }

        protected void WriteETag(long etag)
        {
            HttpContext.Response.Headers[Constants.Headers.Etag] = "\"" + etag + "\"";
        }

        private void WriteEmbeddedFileNotFound(string docPath)
        {
            var message = "The following embedded file was not available: " + docPath +
                          ". Please make sure that the Raven.Studio.zip file exist in the main directory (near the Raven.Server.exe).";
            HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
            HttpContext.Response.Body.Write(message);
        }
    }
}
