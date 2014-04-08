using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using System.Web.Http;

namespace Raven.Database.Server.Controllers
{

    [Route("countergroups/{action=CounterGroupsGet}")]
    public class CounterGroupsController : RavenDbApiController
    {
        [HttpGet]
        public HttpResponseMessage CounterGroupsGet()
        {
            return Request.CreateResponse(HttpStatusCode.OK, new List<string>
            {
                "users",
                "ads",
                "american idol",
                "the voice"
            });
        }
    }
}
