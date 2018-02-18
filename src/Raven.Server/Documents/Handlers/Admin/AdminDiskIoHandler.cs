using System;
using System.Net;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Org.BouncyCastle.Security;
using Raven.Client.Documents.Operations;
using Raven.Client.ServerWide;
using Raven.Server.Documents.Indexes.Static.Extensions;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Raven.Server.Web;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Platform.Posix.macOS;

namespace Raven.Server.Documents.Handlers.Admin
{

    public class AdminDiskIoHandler : RequestHandler
    {
        [RavenAction("/admin/ioTest", "POST", AuthorizationStatus.Operator)]
        public async Task IoTest()
        {
            using (Server.ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                AbstractPerformanceTestRequest ioTestRequest;
                using (var ioTestRequestJson = await context.ReadForMemoryAsync(RequestBodyStream(), "IoTest/Read-Body"))
                {
                    if (!ioTestRequestJson.TryGet("TestType", out string testType))
                    {
                        throw new InvalidParameterException("TestType property was not found in the request body. It is a required property.");
                    }

                    switch (testType)
                    {
                        case GenericPerformanceTestRequest.Mode:
                            ioTestRequest = JsonDeserializationServer.GenericPerformanceTestRequest(ioTestRequestJson);
                            break;
                        case BatchPerformanceTestRequest.Mode:
                            ioTestRequest = JsonDeserializationServer.BatchPerformanceTestRequest(ioTestRequestJson);
                            break;
                        default:
                            //TODO: don't forget logging here
                            HttpContext.Response.StatusCode = (int)HttpStatusCode.InternalServerError;
                            return;
                    }
                    //Database.Documents.Delete(AbstractDiskPerformanceTester.PerformanceResultDocumentKey, null, null);
                    
                    
                    var operationCancelToken = CreateOperationToken();
                    var performanceTester = AbstractDiskPerformanceTester.ForRequest(ioTestRequest);
                    var operationId = ServerStore.Operations.GetNextOperationId();
                    
                    var _ = ServerStore.Operations.AddOperation(null,
                        "Storage I/O Test",
                        Operations.Operations.OperationType.IOTest,
                        progress => Task.Run(() => performanceTester.TestDiskIO(progress)),
                        operationId,
                        operationCancelToken);
                    
                    using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                    {
                        writer.WriteOperationId(context, operationId);
                    }
                }
            }
        }

        protected OperationCancelToken CreateOperationToken()
        {
            return new OperationCancelToken(ServerStore.ServerShutdown);
        }
       

      
    }
}
