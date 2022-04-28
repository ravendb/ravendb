using System.Threading.Tasks;
using JetBrains.Annotations;
using Microsoft.AspNetCore.Http.Features.Authentication;
using Raven.Client.Exceptions.Security;
using Raven.Server.Config;
using Raven.Server.Web;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Admin.Processors.Indexes;

internal abstract class AbstractAdminIndexHandlerProcessorForJavaScriptPut<TRequestHandler, TOperationContext> : AbstractAdminIndexHandlerProcessorForPut<TRequestHandler, TOperationContext>
    where TOperationContext : JsonOperationContext 
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
{
    protected AbstractAdminIndexHandlerProcessorForJavaScriptPut([NotNull] TRequestHandler requestHandler)
        : base(requestHandler, validatedAsAdmin: false)
    {
    }

    protected abstract RavenConfiguration GetDatabaseConfiguration();

    public override async ValueTask ExecuteAsync()
    {
        if (HttpContext.Features.Get<IHttpAuthenticationFeature>() is RavenServer.AuthenticateConnection feature && GetDatabaseConfiguration().Indexing.RequireAdminToDeployJavaScriptIndexes)
        {
            if (feature.CanAccess(RequestHandler.DatabaseName, requireAdmin: true, requireWrite: true) == false)
                throw new AuthorizationException("Deployments of JavaScript indexes has been restricted to admin users only");
        }

        await base.ExecuteAsync();
    }
}
