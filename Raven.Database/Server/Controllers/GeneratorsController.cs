using Raven.Database.Impl.Generators;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using System.Web.Http;

namespace Raven.Database.Server.Controllers
{
    public class GeneratorsController : RavenDbApiController
    {
        [HttpGet]
        [Route("generate/code")]
        [Route("databases/{databaseName}/generate/code")]
        public HttpResponseMessage GenerateCodeFromDocument([FromUri] string doc, [FromUri] string lang = "csharp")
        {
            var msg = GetEmptyMessage();
            if (Database == null)
            {
                msg.StatusCode = HttpStatusCode.NotFound;
                return msg;
            }
            var document = Database.Documents.Get(doc, GetRequestTransaction());
            if (document == null)
            {
                msg.StatusCode = HttpStatusCode.NotFound;
                return msg;
            }

            if (lang.ToLowerInvariant().Trim() != "csharp")
            {
                msg.StatusCode = HttpStatusCode.NotImplemented;
                return msg;
            }
          
            Debug.Assert(document.Etag != null);

            var generator = new JsonCodeGenerator(lang);
            var code = generator.Execute(document);

            return this.GetMessageWithString(code);
        }
    }
}
