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
using Raven.Client.Extensions;
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
            {".html", "text/html"},
            {".htm", "text/html"},
            {".css", "text/css"},
            {".js", "text/javascript"},
            {".ico", "image/vnd.microsoft.icon"},
            {".jpg", "image/jpeg"},
            {".gif", "image/gif"},
            {".png", "image/png"},
            {".xap", "application/x-silverlight-2"},
            {".json", "application/json"},
            {".eot", "application/vnd.ms-fontobject"},
            {".svg", "image/svg+xml"},
            {".ttf", "application/octet-stream"},
            {".woff", "application/font-woff"},
            {".woff2", "application/font-woff2"},
            {".appcache", "text/cache-manifest"}
        };

        private static FileInfo TryGetFileName(string basePath, string fileName)
        {
            // this is expected to run concurrently
            FileInfo value;
            if (_fileNamesCaseInsensitive.TryGetValue(fileName, out value))
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

            using (var data = File.OpenRead(file.FullName))
            {
                await data.CopyToAsync(HttpContext.Response.Body, 16 * 1024);
            }
        }

        [RavenAction("/studio/$", "GET", NoAuthorizationRequired = true)]
        public async Task GetStudioFile()
        {
            var fileName = new StringSegment(
                RouteMatch.Url,
                RouteMatch.Url.Length - RouteMatch.MatchLength, RouteMatch.MatchLength);

            var env = (IHostingEnvironment)HttpContext.RequestServices.GetService(typeof(IHostingEnvironment));

            var basePath = Server.Configuration.Core.StudioDirectory ?? env.ContentRootPath;

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

        [RavenAction("/", "GET")]
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
            string contentType;
            return FileExtensionToContentTypeMapping.TryGetValue(fileExtension, out contentType)
                ? contentType
                : "text/plain";
        }

        public string GetHeader(string key)
        {
            StringValues values;
            var requestHeaders = HttpContext.Request.Headers;
            if (requestHeaders.TryGetValue(key, out values))
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
