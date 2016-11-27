// -----------------------------------------------------------------------
//  <copyright file="Studio.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Raven.Server.Routing;
using Raven.Server.Utils;
using Sparrow;

namespace Raven.Server.Web.System
{
    public class StudioHandler : RequestHandler
    {
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

            HttpContext.Response.StatusCode = 404;
        }

        [RavenAction("/", "GET")]
        public Task RavenRoot()
        {
            HttpContext.Response.Headers["Location"] = "/studio/index.html";
            HttpContext.Response.StatusCode = 301;
            return Task.CompletedTask;
        }
    }
}