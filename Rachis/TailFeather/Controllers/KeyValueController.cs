using System;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using System.Web.Http;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Rachis;
using Rachis.Transport;
using Rachis.Utils;
using TailFeather.Storage;

namespace TailFeather.Controllers
{
    public class KeyValueController : TailFeatherController
    {
        [HttpGet]
        [Route("tailfeather/key-val/read")]
        public async Task<HttpResponseMessage> Read([FromUri] string key, [FromUri] string mode = null)
        {
            switch (mode)
            {
                case "quorum":
                    var taskCompletionSource = new TaskCompletionSource<object>();
                    try
                    {
                        RaftEngine.AppendCommand(new GetCommand
                        {
                            Key = key,
                            Completion = taskCompletionSource
                        });
                    }
                    catch (NotLeadingException e)
                    {
                        return RedirectToLeader(e.CurrentLeader, Request.RequestUri);
                    }
                    var consistentRead = await taskCompletionSource.Task;
                    return Request.CreateResponse(HttpStatusCode.OK, new
                    {
                        RaftEngine.State,
                        Key = key,
                        Value = consistentRead,
                        Missing = consistentRead == null
                    });
                case "leader":
                    if (RaftEngine.State != RaftEngineState.Leader)
                    {
                        return RedirectToLeader(RaftEngine.CurrentLeader, Request.RequestUri);
                    }
                    goto case null;
                case "any":
                case null:
                    var read = StateMachine.Read(key);
                    return Request.CreateResponse(HttpStatusCode.OK, new
                    {
                        RaftEngine.State,
                        Key = key,
                        Value = read,
                        Missing = read == null
                    });
                default:
                    return Request.CreateResponse(HttpStatusCode.BadRequest, new
                    {
                        Error = "Unknown read mode"
                    });
            }

        }

        private HttpResponseMessage RedirectToLeader(string currentLeader, Uri baseUrl)
        {
            var leaderNode = RaftEngine.CurrentTopology.AllNodes.FirstOrDefault(x => { return x.Name == currentLeader; });
            if (leaderNode == null)
            {
                return Request.CreateResponse(HttpStatusCode.BadRequest, new
                {
                    Error = "There is no current leader, try again later"
                });
            }
            var httpResponseMessage = Request.CreateResponse(HttpStatusCode.Redirect);
            httpResponseMessage.Headers.Location = new UriBuilder(leaderNode.Uri)
            {
                Path = baseUrl.LocalPath,
                Query = baseUrl.Query,
                Fragment = baseUrl.Fragment
            }.Uri;
            return httpResponseMessage;
        }

        [HttpGet]
        [Route("tailfeather/key-val/set")]
        public Task<HttpResponseMessage> Set([FromUri] string key, [FromUri] string val)
        {
            JToken jVal;
            try
            {
                jVal = JToken.Parse(val);
            }
            catch (JsonReaderException)
            {
                jVal = val;
            }

            var op = new KeyValueOperation
            {
                Key = key,
                Type = KeyValueOperationTypes.Add,
                Value = jVal
            };

            return Batch(new[] { op });
        }

        [HttpGet]
        [Route("tailfeather/key-val/del")]
        public Task<HttpResponseMessage> Del([FromUri] string key)
        {
            var op = new KeyValueOperation
            {
                Key = key,
                Type = KeyValueOperationTypes.Del,
            };

            return Batch(new[] { op });
        }

        [HttpPost]
        [Route("tailfeather/key-val/batch")]
        public async Task<HttpResponseMessage> Batch()
        {
            var stream = await Request.Content.ReadAsStreamAsync();
            var operations = new JsonSerializer().Deserialize<KeyValueOperation[]>(new JsonTextReader(new StreamReader(stream)));

            return await Batch(operations);
        }

        private async Task<HttpResponseMessage> Batch(KeyValueOperation[] operations)
        {
            var taskCompletionSource = new TaskCompletionSource<object>();
            try
            {
                RaftEngine.AppendCommand(new OperationBatchCommand
                {
                    Batch = operations,
                    Completion = taskCompletionSource
                });
            }
            catch (NotLeadingException e)
            {
                return RedirectToLeader(e.CurrentLeader, Request.RequestUri);
            }
            await taskCompletionSource.Task;

            return Request.CreateResponse(HttpStatusCode.Accepted);
        }
    }
}