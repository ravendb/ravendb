using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Web;
using Raven.Server.Web.Studio;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.SampleData
{
    internal abstract class AbstractSampleDataHandlerProcessorForGetSampleDataClasses<TRequestHandler, TOperationContext> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
        where TOperationContext : JsonOperationContext 
        where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
    {
        protected AbstractSampleDataHandlerProcessorForGetSampleDataClasses([NotNull] TRequestHandler requestHandler) : base(requestHandler)
        {
        }

        public override async ValueTask ExecuteAsync()
        {
            await using (var sampleData = typeof(SampleDataHandler).Assembly.GetManifestResourceStream("Raven.Server.Web.Studio.EmbeddedData.NorthwindModel.cs"))
            await using (var responseStream = RequestHandler.ResponseBodyStream())
            {
                HttpContext.Response.ContentType = "text/plain";
                await sampleData.CopyToAsync(responseStream);
            }
        }
    }
}
