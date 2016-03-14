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

namespace Raven.Server.Web.System
{

    public class StudioHandler : RequestHandler
    {
        private static readonly Dictionary<string, string> MimeMapping = new Dictionary<string, string>()
        {
            { "css", "text/css"},
            { "js", "application/javascript" },
            { "json", "application/javascript" }
        };

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

            var ravenPath = $"~/../../Raven.Studio/wwwroot/{filename}";

#if DEBUG
            ravenPath = Path.GetFullPath(ravenPath).Replace($"{Path.DirectorySeparatorChar}test{Path.DirectorySeparatorChar}", $"{Path.DirectorySeparatorChar}src{Path.DirectorySeparatorChar}");
#endif

            if (File.Exists(ravenPath))
            {
                await WriteFile(ravenPath);
                return;
            }
            HttpContext.Response.StatusCode = 404;
            return;

            //filePath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "../Raven.Studio/", docPath);
            //if (File.Exists(filePath))
            //    return WriteFile(filePath);

            //filePath = Path.Combine(this.SystemConfiguration.EmbeddedFilesDirectory, docPath);
            //if (File.Exists(filePath))
            //    return WriteFile(filePath);

            //filePath = Path.Combine("~/../../../../Raven.Studio.Html5", docPath);
            //if (File.Exists(filePath))
            //    return WriteFile(filePath);

            //if (string.IsNullOrEmpty(zipPath) == false)
            //{
            //    var fullZipPath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, zipPath + ".zip");

            //    if (File.Exists(fullZipPath) == false)
            //        fullZipPath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "bin", zipPath + ".zip");

            //    if (File.Exists(fullZipPath) == false)
            //        fullZipPath = Path.Combine(this.SystemConfiguration.EmbeddedFilesDirectory, zipPath + ".zip");

            //    if (File.Exists(fullZipPath))
            //    {
            //        return WriteFileFromZip(fullZipPath, docPath);
            //    }
            //}
            //await WriteFile(filepath);
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