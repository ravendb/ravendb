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
    [Route("counters/{action=CountersGet}")]
    public class CountersController : RavenDbApiController
    {
        [HttpGet]
        public HttpResponseMessage CountersGet(int skip, int take, string counterGroupName)
        {
            return Request.CreateResponse(HttpStatusCode.OK, new List<Counter>{
                    new Counter{
                        Name="User A",
                        OverallTotal= 0,
                        Servers = new List<Server>{
                           new Server{
                               Name="svr1",
                               PosCount= 4,
                               NegCount= -2
                           },
                           new Server{
                               Name="svr2",
                               PosCount=10,
                               NegCount=-12
                           },
                           new Server{
                               Name="svr3",
                               PosCount=30,
                               NegCount= -5
                           },
                        }
                    },
                    new Counter{
                        Name="User B",
                        OverallTotal= 0,
                        Servers = new List<Server>{
                           new Server{
                               Name="svr1",
                               PosCount= 4,
                               NegCount= -2
                           },
                           new Server{
                               Name="svr2",
                               PosCount=10,
                               NegCount=-12
                           },
                           new Server{
                               Name="svr3",
                               PosCount=30,
                               NegCount= -5
                           },
                        }
                    }
            });
        }
    }

    public class CounterGroup
    {
        public string Name { get; set; }
        public List<Counter> Counters { get; set; }
    }
    public class Counter
    {
        public string Name { get; set; }
        public int OverallTotal { get; set; }
        public List<Server> Servers { get; set; }
    }

    public class Server
    {
        public string Name { get; set; }
        public long PosCount { get; set; }
        public long NegCount { get; set; }
    }
}
