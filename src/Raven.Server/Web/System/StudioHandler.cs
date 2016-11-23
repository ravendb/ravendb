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
using System.Net.Http;
using System.Reflection;
using System.Runtime.Loader;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Hosting;
using Raven.Abstractions.Data;
using Raven.Client.Connection;
using Raven.Server.Routing;
using Raven.Server.Utils;
using Raven.Abstractions.Extensions;
using System.Linq;
using System.Net.Http.Headers;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Primitives;
using StringSegment = Sparrow.StringSegment;

namespace Raven.Server.Web.System
{
    public class StudioHandler : RequestHandler
    {
        private static readonly ConcurrentDictionary<string, string> ZipLastChangedDate =
                new ConcurrentDictionary<string, string>(StringComparer.OrdinalIgnoreCase);

        private static readonly Dictionary<string, string> MimeMapping = new Dictionary<string, string>()
        {
            {"css", "text/css"},
            {"png", "image/png"},
            {"svg", "image/svg+xml"},
            {"js", "application/javascript"},
            {"json", "application/javascript"}
        };

        private static DateTime _lastFileNamesUpdate = DateTime.MinValue;
        private static Dictionary<string, string> _fileNamesCaseInsensitive =
            new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

        public static string[] LookupPaths = new[]
        {
            "~/../../Raven.Studio/wwwroot",
            "~/../src/Raven.Studio/wwwroot",

        };

        private static string TryGetFileName(string filename)
        {
            // this is expected to run concurrently
            string value;
            if (_fileNamesCaseInsensitive.TryGetValue(filename, out value))
                return value;

            if ((DateTime.UtcNow - _lastFileNamesUpdate).TotalSeconds < 3)
                return null;

            var files = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            foreach (var lookupPath in LookupPaths)
            {
                if (Directory.Exists(lookupPath) == false)
                    continue;

                foreach (var file in Directory.GetFiles(lookupPath,"*",SearchOption.AllDirectories))
                {
                    files[file.Substring(lookupPath.Length+1).Replace("\\","/")] = file;
                }
            }
            _lastFileNamesUpdate = DateTime.UtcNow;
            if (files.TryGetValue(filename, out value))
            {
                // only replace the value if there is a point to it, note that concurrent threads
                // may create it multiple times, until it is settled
                _fileNamesCaseInsensitive = files;
                return value;
            }
            return null;

        }

        //TODO: write better impl for this! - it is temporary solution to make studio work properly
        private string FindMimeType(string filePath)
        {
            var extension = filePath.Substring(filePath.LastIndexOf('.') + 1);

            string mime;

            if (MimeMapping.TryGetValue(extension, out mime))
            {
                return mime;
            }
            return "text/html";
        }

        public async Task WriteFile(string filePath)
        {
#if DEBUG
            HttpContext.Response.Headers["File-Path"] = Path.GetFullPath(filePath);
#endif

            var etagValue = HttpContext.Request.Headers["If-None-Match"];

            var fileEtag = '"' + File.GetLastWriteTimeUtc(filePath).ToString("G") + '"';
            if (etagValue == fileEtag)
            {
                HttpContext.Response.StatusCode = 304; // Not Modified
                return;
            }

            HttpContext.Response.ContentType = FindMimeType(filePath);
            HttpContext.Response.Headers["ETag"] = fileEtag;

            using (var data = File.OpenRead(filePath))
            {
                await data.CopyToAsync(HttpContext.Response.Body, 16*1024);
            }
        }

        [RavenAction("/studio/$", "GET", NoAuthorizationRequired = true)]
        public async Task GetStudioFile()
        {
            var filename = new StringSegment(
                RouteMatch.Url,
                RouteMatch.MatchLength,
                RouteMatch.Url.Length - RouteMatch.MatchLength);

            var file = TryGetFileName(filename);

            if (file != null)
            {
                await WriteFile(file);
                return;
            }

            var env = (IHostingEnvironment)HttpContext.RequestServices.GetService(typeof(IHostingEnvironment));
            var basePath = env.ContentRootPath;
            var zipFilePath = Path.Combine(basePath, "Raven.Studio.zip");

            if (File.Exists(zipFilePath) == false)
            {
                HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                return;
            }

            WriteFileFromZip(zipFilePath, filename);
        }

        [RavenAction("/", "GET")]
        public Task RavenRoot()
        {
            HttpContext.Response.Headers["Location"] = "/studio/index.html";
            HttpContext.Response.StatusCode = 301;
            return Task.CompletedTask;
        }

        private void WriteFileFromZip(string zipPath, string docPath)
        {
            var etagValue = GetHeader("If-None-Match") ?? GetHeader("If-Match");
            var currentFileEtag = ZipLastChangedDate.GetOrAdd(zipPath, f => File.GetLastWriteTime(f).Ticks.ToString("G")) + docPath;
            if (etagValue == $"\"{ currentFileEtag }\"")
            {
                WriteEmptyMessage(HttpStatusCode.NotModified);
                return;
            }

            var fileStream = new FileStream(zipPath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
            var zipArchive = new ZipArchive(fileStream, ZipArchiveMode.Read, false);

            var zipEntry = zipArchive.Entries.FirstOrDefault(a => a.FullName.Equals(docPath, StringComparison.OrdinalIgnoreCase));
            if (zipEntry == null)
            {
                WriteEmbeddedFileNotFound(docPath);
                return;
            }

            using (var responseStream = ResponseBodyStream())
            {
                var type = GetContentType(docPath);
                HttpContext.Response.ContentType = new MediaTypeHeaderValue(type).ToString();
                WriteETag(currentFileEtag);
                using (var entry = zipEntry.Open())
                {
                    entry.CopyTo(responseStream);
                    entry.Flush();
                }
            }
        }

        private static string GetContentType(string docPath)
        {
            switch (Path.GetExtension(docPath))
            {
                case ".html":
                case ".htm":
                    return "text/html";
                case ".css":
                    return "text/css";
                case ".js":
                    return "text/javascript";
                case ".ico":
                    return "image/vnd.microsoft.icon";
                case ".jpg":
                    return "image/jpeg";
                case ".gif":
                    return "image/gif";
                case ".png":
                    return "image/png";
                case ".xap":
                    return "application/x-silverlight-2";
                case ".json":
                    return "application/json";
                case ".eot":
                    return "application/vnd.ms-fontobject";
                case ".svg":
                    return "image/svg+xml";
                case ".ttf":
                    return "application/octet-stream";
                case ".woff":
                    return "application/font-woff";
                case ".woff2":
                    return "application/font-woff2";
                case ".appcache":
                    return "text/cache-manifest";
                default:
                    return "text/plain";
            }
        }

        public string GetHeader(string key)
        {
            StringValues values;
            var requestHeaders = HttpContext.Request.Headers;
            if (requestHeaders.TryGetValue(key, out values))
                return values.FirstOrDefault();
            return null;
        }

        public virtual void WriteEmptyMessage(HttpStatusCode code = HttpStatusCode.OK, long? etag = null)
        {
            HttpContext.Response.StatusCode = (int)code;
            WriteETag(etag);
        }

        protected void WriteETag(long? etag)
        {
            WriteETag(etag.ToInvariantString());
        }

        protected void WriteETag(string etag)
        {
            HttpContext.Response.Headers[Constants.MetadataEtagField] = etag.ToInvariantString();
        }

        private void WriteEmbeddedFileNotFound(string docPath)
        {
            var message = "The following embedded file was not available: " + docPath +
                          ". Please make sure that the Raven.Studio.zip file exist in the main directory (near to the Raven.Database.dll).";
            HttpContext.Response.StatusCode = (int) HttpStatusCode.NotFound;
            HttpContext.Response.ContentType = "application/json";
            HttpContext.Response.Body.Write(message);
        }
    }
}
