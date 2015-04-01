using System;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Runtime.CompilerServices;
using System.Security.Principal;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Util;
using Raven.Database.Extensions;
using Raven.Database.Server.Security;

namespace Raven.Database.Server.Controllers.Admin
{
	public abstract class BaseAdminController : RavenDbApiController
	{
        protected virtual WindowsBuiltInRole[] AdditionalSupportedRoles
        {
            get
            {
                return new WindowsBuiltInRole[0];
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected bool IsSystemDatabase(string databaseId)
        {
            return databaseId.Equals(Constants.SystemDatabase, StringComparison.OrdinalIgnoreCase);
        }

        public override async Task<HttpResponseMessage> ExecuteAsync(System.Web.Http.Controllers.HttpControllerContext controllerContext, System.Threading.CancellationToken cancellationToken)
        {
            InnerInitialization(controllerContext);
            var authorizer = (MixedModeRequestAuthorizer)controllerContext.Configuration.Properties[typeof(MixedModeRequestAuthorizer)];

            HttpResponseMessage authMsg;
            if (authorizer.TryAuthorize(this, out authMsg) == false)
                return authMsg;

            var accessMode = DatabasesLandlord.SystemConfiguration.AnonymousUserAccessMode;
            if (accessMode == AnonymousUserAccessMode.Admin || accessMode == AnonymousUserAccessMode.All ||
            (accessMode == AnonymousUserAccessMode.Get && InnerRequest.Method.Method == "GET"))
                return await base.ExecuteAsync(controllerContext, cancellationToken);

            var user = authorizer.GetUser(this);
            if (user == null)
                return GetMessageWithObject(new
                {
                    Error = "The operation '" + GetRequestUrl() + "' is only available to administrators, and could not find the user to authorize with"
                }, HttpStatusCode.Unauthorized);

            if (user.IsAdministrator(DatabasesLandlord.SystemConfiguration.AnonymousUserAccessMode) == false &&
            user.IsAdministrator(Database) == false && SupportedByAnyAdditionalRoles(user) == false)
            {
                return GetMessageWithObject(new
                {
                    Error = "The operation '" + GetRequestUrl() + "' is only available to administrators"
                }, HttpStatusCode.Unauthorized);
            }

            return await base.ExecuteAsync(controllerContext, cancellationToken);
        }

        private bool SupportedByAnyAdditionalRoles(IPrincipal user)
        {
            return AdditionalSupportedRoles.Any(role => user.IsInRole(DatabasesLandlord.SystemConfiguration.AnonymousUserAccessMode, role));
        }

        protected static MessageWithStatusCode CheckNameFormat(string name, string dataDirectory)
        {
            string errorMessage = null;
            const HttpStatusCode errorCode = HttpStatusCode.BadRequest;

            if (name == null)
            {
                errorMessage = "An empty name is forbidden for use!";
            }
            else if (name.IndexOfAny(Path.GetInvalidFileNameChars()) >= 0)
            {
                errorMessage = string.Format("The name '{0}' contains characters that are forbidden for use!", name);
            }
            else if (Array.IndexOf(Constants.WindowsReservedFileNames, name.ToLower()) >= 0)
            {
                errorMessage = string.Format("The name '{0}' is forbidden for use!", name);
            }
            else if ((Environment.OSVersion.Platform == PlatformID.Unix) && (name.Length > Constants.LinuxMaxFileNameLength) && (dataDirectory.Length + name.Length > Constants.LinuxMaxPath))
            {
                int theoreticalMaxFileNameLength = Constants.LinuxMaxPath - dataDirectory.Length;
                int maxfileNameLength = (theoreticalMaxFileNameLength > Constants.LinuxMaxFileNameLength) ? Constants.LinuxMaxFileNameLength : theoreticalMaxFileNameLength;
                errorMessage = string.Format("Invalid name! Name cannot exceed {0} characters", maxfileNameLength);
            }
            else if (Path.Combine(dataDirectory, name).Length > Constants.WindowsMaxPath)
            {
                int maxfileNameLength = Constants.WindowsMaxPath - dataDirectory.Length;
                errorMessage = string.Format("Invalid name! Name cannot exceed {0} characters", maxfileNameLength);
            }

            return new MessageWithStatusCode { Message = errorMessage, ErrorCode = errorCode };
        }

        protected class MessageWithStatusCode
        {
            public string Message;
            public HttpStatusCode ErrorCode = HttpStatusCode.OK;
        }
	}
}