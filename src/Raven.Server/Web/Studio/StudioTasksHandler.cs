using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using Jurassic;
using Microsoft.AspNetCore.Http;
using Raven.Client;
using Raven.Server.Documents.Patch;
using Raven.Server.Routing;
using Sparrow.Json;

namespace Raven.Server.Web.Studio
{
    public class StudioTasksHandler : RequestHandler
    {
        //TODO: split to specific handler
        [RavenAction("/databases/*/studio-tasks/config", "GET", AuthorizationStatus.ValidUser)]
        public Task Config()
        {
            //TODO: implement
            HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
            return Task.CompletedTask;
        }

        //TODO: split to specific handler
        [RavenAction("/studio-tasks/server-configs", "GET", AuthorizationStatus.ValidUser)]
        public Task Get()
        {
            //TODO: implement
            return HttpContext.Response.WriteAsync("{\"IsGlobalAdmin\":true,\"CanReadWriteSettings\":true,\"CanReadSettings\":true,\"CanExposeConfigOverTheWire\":true}");
        }

        [RavenAction("/studio-tasks/new-encryption-key", "GET", AuthorizationStatus.ValidUser)]
        public async Task GetNewEncryption()
        {
            RandomNumberGenerator randomNumberGenerator = RandomNumberGenerator.Create();
            var byteStruct = new byte[Constants.Documents.Encryption.DefaultGeneratedEncryptionKeyLength];
            randomNumberGenerator.GetBytes(byteStruct);
            var result = Convert.ToBase64String(byteStruct);

            await HttpContext.Response.WriteAsync($"\"{result}\"", Encoding.UTF8);
        }

        [RavenAction("/studio-tasks/is-base-64-key", "POST", AuthorizationStatus.ValidUser)]
        public Task IsBase64Key()
        {
            StreamReader reader = new StreamReader(HttpContext.Request.Body);
            string keyU = reader.ReadToEnd();
            string key = Uri.UnescapeDataString(keyU);
            try
            {
                Convert.FromBase64String(key.Substring(4));
            }
            catch (Exception)
            {
                HttpContext.Response.StatusCode = (int)HttpStatusCode.BadRequest; // Bad Request
                return HttpContext.Response.WriteAsync("\"The key must be in Base64 encoding format!\"");
            }

            HttpContext.Response.WriteAsync("\"The key is ok!\"");
            return Task.CompletedTask;
        }


        public class FuncitonValidation
        {
            public List<string> Functions;
        }
    }
}
