using System.IO;
using Microsoft.AspNet.Mvc;
using Raven.Abstractions;
using Raven.Imports.Newtonsoft.Json;
using Raven.Imports.Newtonsoft.Json.Linq;
using Raven.Json.Linq;

namespace Raven.Server.Utils.Web
{
    public class JsonActionResult : ActionResult
    {
        private readonly RavenJObject _val;

        public JsonActionResult(RavenJObject val)
        {
            _val = val;
        }

        // TODO: implement this using blittable?
        //public override Task ExecuteResultAsync(ActionContext context)
        //{
            
        //}

        public override void ExecuteResult(ActionContext context)
        {
            if (_val == null || _val.Type == JTokenType.Null || _val.Type == JTokenType.Undefined)
            {
                return;
            }
            context.HttpContext.Response.StatusCode = 200;
            bool isBrowser = false;
            foreach (var userAgent in context.HttpContext.Request.Headers["User-Agent"])
            {
                if (!userAgent.Contains("Mozilla")) continue;
                isBrowser = true;
                break;
            }
            context.HttpContext.Response.ContentType = "application/json";
            var streamWriter = new StreamWriter(context.HttpContext.Response.Body);
            _val.WriteTo(new JsonTextWriter(streamWriter)
            {
                Formatting = isBrowser ? Formatting.Indented : Formatting.None,
            }, Default.Converters);
            streamWriter.Flush();
        }
    }
}