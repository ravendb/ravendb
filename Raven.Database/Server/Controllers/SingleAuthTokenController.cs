using System;
using System.Net;
using System.Net.Http;
using System.Web.Http;
using Raven.Database.Extensions;
using Raven.Database.Server.Security;
using Raven.Database.Server.WebApi.Attributes;

namespace Raven.Database.Server.Controllers
{
    public class DbSingleAuthTokenController : RavenDbApiController
    {
        [HttpGet]
        [RavenRoute("singleAuthToken")]
        [RavenRoute("databases/{databaseName}/singleAuthToken")]
        public HttpResponseMessage SingleAuthGet()
        {
            var authorizer = (MixedModeRequestAuthorizer) ControllerContext.Configuration.Properties[typeof (MixedModeRequestAuthorizer)];
            bool shouldCheckIfMachineAdmin = false;


            if ((DatabaseName == "<system>" || string.IsNullOrEmpty(DatabaseName)) && bool.TryParse(GetQueryStringValue("CheckIfMachineAdmin"), out shouldCheckIfMachineAdmin) && shouldCheckIfMachineAdmin)
            {

                if (!User.IsAdministrator(AnonymousUserAccessMode.None) &&
                    DatabasesLandlord.SystemConfiguration.AnonymousUserAccessMode != AnonymousUserAccessMode.Admin)
                {
                    return GetMessageWithObject(new
                    {
                        Reason = "User is null or not authenticated"
                    }, HttpStatusCode.Unauthorized);
                }

            }

            var token = authorizer.GenerateSingleUseAuthToken(DatabaseName, User);

            return GetMessageWithObject(new
            {
                Token = token
            });
        }
    }
}

namespace Raven.Database.FileSystem.Controllers
{

    public class FsSingleAuthTokenController : RavenFsApiController
    {
        [HttpGet]
        [RavenRoute("fs/{fileSystemName}/singleAuthToken")]
        public HttpResponseMessage SingleAuthGet()
        {
            var authorizer = (MixedModeRequestAuthorizer) ControllerContext.Configuration.Properties[typeof (MixedModeRequestAuthorizer)];

            var token = authorizer.GenerateSingleUseAuthToken("fs/" + FileSystemName, User);

            return GetMessageWithObject(new
            {
                Token = token
            });
        }
    }
}

namespace Raven.Database.Counters.Controllers
{
    public class CounterSingleAuthTokenController : RavenCountersApiController
    {
        [HttpGet]
        [RavenRoute("cs/{counterStorageName}/singleAuthToken")]
        public HttpResponseMessage SingleAuthGet()
        {
            var authorizer = (MixedModeRequestAuthorizer) ControllerContext.Configuration.Properties[typeof (MixedModeRequestAuthorizer)];

            var token = authorizer.GenerateSingleUseAuthToken("cs/" + CounterStorageName, User);

            return GetMessageWithObject(new
            {
                Token = token
            });
        }
    }
}
