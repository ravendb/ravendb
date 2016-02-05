// -----------------------------------------------------------------------
//  <copyright file="Studio.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading.Tasks;
using Microsoft.AspNet.Http;
using Raven.Client.Connection;
using Raven.Server.Documents;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide;

namespace Raven.Server.Web.System
{

    public class Studio : RequestHandler
    {
        public async Task WriteFile(string filePath)
        {
            var etagValue = HttpContext.Request.Headers["If-None-Match"].FirstOrDefault() ??
                            HttpContext.Request.Headers["If-Match"].FirstOrDefault();
            if (etagValue != null)
            {
                // Bug fix: the etag header starts and ends with quotes, resulting in cache-busting; the Studio always receives new files, even if should be cached.
                etagValue = etagValue.Trim(new[] {'\"'});
            }

            var fileEtag = File.GetLastWriteTimeUtc(filePath).ToString("G");
            if (etagValue == fileEtag)
            {
                HttpContext.Response.StatusCode = 304; // Not Modified
                return;
            }

            HttpContext.Response.ContentType = "text/html";

            using (var data = File.OpenRead(filePath))
            {
                await data.CopyToAsync(HttpContext.Response.Body, 16*1024);
            }
        }

        [Route("/studio/$", "GET")]
        public async Task GetStudioFile()
        {
            var filename = new StringSegment(this.RouteMatch.Url, this.RouteMatch.MatchLength,
                this.RouteMatch.Url.Length - this.RouteMatch.MatchLength);

            var ravenPath = $"~/../../Raven.Studio/{filename}";
            var docPath = "index.html";

            var filePath = Path.Combine(ravenPath, docPath);
            if (File.Exists(ravenPath))
            {
                await WriteFile(ravenPath);
                return;
            }

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

        [Route("/", "GET")]
        public Task RavenRoot()
        {
            const string rootPath = "/studio/index.html";
            HttpContext.Response.Headers["Location"] = rootPath;
            HttpContext.Response.StatusCode = 301;
            return Task.CompletedTask;
        }
    }
}