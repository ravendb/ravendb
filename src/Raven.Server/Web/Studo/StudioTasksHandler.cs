using System;
using System.IO;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Server.Routing;
using Microsoft.AspNetCore.Http;

namespace Raven.Server.Web.Studo
{
    public class StudioTasksHandler : RequestHandler
    {
        [RavenAction("/databases/*/studio-tasks/config", "GET")]
        public Task Config()
        {
            //TODO: implement
            HttpContext.Response.StatusCode = 404;
            return Task.CompletedTask;
        }

        [RavenAction("/studio-tasks/server-configs", "GET")]
        public Task Get()
        {
            //TODO: implement
            return HttpContext.Response.WriteAsync("{\"IsGlobalAdmin\":true,\"CanReadWriteSettings\":true,\"CanReadSettings\":true,\"CanExposeConfigOverTheWire\":true}");
        }

        [RavenAction("/studio-tasks/new-encryption-key", "GET")]
        public async Task GetNewEncryption()
        {
            RandomNumberGenerator randomNumberGenerator = RandomNumberGenerator.Create();
            var byteStruct = new byte[Constants.DefaultGeneratedEncryptionKeyLength];
            randomNumberGenerator.GetBytes(byteStruct);
            var result = Convert.ToBase64String(byteStruct);

            HttpContext.Response.StatusCode = 200;
            HttpContext.Response.Headers["Content-Type"] = "application/json; charset=utf-8";
            await HttpContext.Response.WriteAsync($"\"{result}\"", Encoding.UTF8);
        }

        [RavenAction("/studio-tasks/is-base-64-key", "POST")]
        public Task IsBase64Key()
        {
            StreamReader reader = new StreamReader(HttpContext.Request.Body);
            string keyU = reader.ReadToEnd();
            string key = Uri.UnescapeDataString(keyU);
            HttpContext.Response.Headers["Content-Type"] = "application/json; charset=utf-8";
            try
            {
                Convert.FromBase64String(key.Substring(4));
            }
            catch (Exception)
            {
                HttpContext.Response.StatusCode = 400; // Bad Request
                return HttpContext.Response.WriteAsync("\"The key must be in Base64 encoding format!\"");
            }
            HttpContext.Response.StatusCode = 200;
            HttpContext.Response.WriteAsync("\"The key is ok!\"");
            return Task.CompletedTask;
        }
    }
}