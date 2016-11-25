// -----------------------------------------------------------------------
//  <copyright file="AdminResourceApiController.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Runtime.CompilerServices;
using System.Security.Principal;
using System.Threading;
using System.Threading.Tasks;
using System.Web.Http.Controllers;

using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Database.Extensions;
using Raven.Database.Server;
using Raven.Database.Server.Security;

namespace Raven.Database.Common
{
    public abstract class AdminResourceApiController<TResource, TResourceLandlord> : ResourceApiController<TResource, TResourceLandlord>
        where TResource : IResourceStore
        where TResourceLandlord : IResourceLandlord<TResource>
    {
        protected virtual WindowsBuiltInRole[] AdditionalSupportedRoles
        {
            get
            {
                return new WindowsBuiltInRole[0];
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected static bool IsSystemDatabase(string databaseId)
        {
            return databaseId.Equals(Constants.SystemDatabase, StringComparison.OrdinalIgnoreCase);
        }

        public override async Task<HttpResponseMessage> ExecuteAsync(HttpControllerContext controllerContext, CancellationToken cancellationToken)
        {
            InnerInitialization(controllerContext);

            HttpResponseMessage msg;
            if (IsClientV4OrHigher(out msg))
                return msg;

            var authorizer = (MixedModeRequestAuthorizer)controllerContext.Configuration.Properties[typeof(MixedModeRequestAuthorizer)];

            HttpResponseMessage authMsg;
            if (authorizer.TryAuthorize(this, out authMsg) == false)
                return authMsg;

            var accessMode = DatabasesLandlord.SystemConfiguration.AnonymousUserAccessMode;
            if (accessMode == AnonymousUserAccessMode.Admin || accessMode == AnonymousUserAccessMode.All ||
            (accessMode == AnonymousUserAccessMode.Get && InnerRequest.Method.Method == "GET"))
                return await base.ExecuteAsync(controllerContext, cancellationToken).ConfigureAwait(false);

            var user = authorizer.GetUser(this);
            if (user == null)
                return GetMessageWithObject(new
                {
                    Error = "The operation '" + GetRequestUrl() + "' is only available to administrators, and could not find the user to authorize with"
                }, HttpStatusCode.Unauthorized);

            if (user.IsAdministrator(DatabasesLandlord.SystemConfiguration.AnonymousUserAccessMode) == false &&
            user.IsAdministrator(ResourceName) == false && SupportedByAnyAdditionalRoles(user) == false)
            {
                return GetMessageWithObject(new
                {
                    Error = "The operation '" + GetRequestUrl() + "' is only available to administrators"
                }, HttpStatusCode.Unauthorized);
            }

            return await base.ExecuteAsync(controllerContext, cancellationToken).ConfigureAwait(false);
        }

        private bool SupportedByAnyAdditionalRoles(IPrincipal user)
        {
            return AdditionalSupportedRoles.Any(role => user.IsInRole(DatabasesLandlord.SystemConfiguration.AnonymousUserAccessMode, role));
        }

        protected static bool IsValidName(string name, string dataDirectory, out MessageWithStatusCode errorMessageWithStatusCode)
        {
            string errorMessage = null;
            bool isValid = true;
            const HttpStatusCode errorCode = HttpStatusCode.BadRequest;

            if (name == null)
            {
                errorMessage = "An empty name is forbidden for use!";
                isValid = false;
            }
            else if (name.IndexOfAny(Path.GetInvalidFileNameChars()) >= 0)
            {
                errorMessage = string.Format("The name '{0}' contains characters that are forbidden for use!", name);
                isValid = false;
            }
            else if (Array.IndexOf(Constants.WindowsReservedFileNames, name.ToLower()) >= 0)
            {
                errorMessage = string.Format("The name '{0}' is forbidden for use!", name);
                isValid = false;
            }
            else if ((Environment.OSVersion.Platform == PlatformID.Unix) && (name.Length > Constants.LinuxMaxFileNameLength) && (dataDirectory.Length + name.Length > Constants.LinuxMaxPath))
            {
                int theoreticalMaxFileNameLength = Constants.LinuxMaxPath - dataDirectory.Length;
                int maxfileNameLength = (theoreticalMaxFileNameLength > Constants.LinuxMaxFileNameLength) ? Constants.LinuxMaxFileNameLength : theoreticalMaxFileNameLength;
                errorMessage = string.Format("Invalid name! Name cannot exceed {0} characters", maxfileNameLength);
                isValid = false;
            }
            else if (Path.Combine(dataDirectory, name).Length > Constants.WindowsMaxPath)
            {
                int maxfileNameLength = Constants.WindowsMaxPath - dataDirectory.Length;
                errorMessage = string.Format("Invalid name! Name cannot exceed {0} characters", maxfileNameLength);
                isValid = false;
            }

            errorMessageWithStatusCode = new MessageWithStatusCode { Message = errorMessage, ErrorCode = errorCode };
            return isValid;
        }

        protected class MessageWithStatusCode
        {
            public string Message;
            public HttpStatusCode ErrorCode = HttpStatusCode.OK;
        }

        protected bool ParseBoolQueryString(string parameterName)
        {
            bool result;
            return bool.TryParse(InnerRequest.RequestUri.ParseQueryString()[parameterName], out result) && result;
        }

        protected bool IsAnotherRestoreInProgress(out string resourceName)
        {
            resourceName = null;
            var restoreDoc = SystemDatabase.Documents.Get(RestoreInProgress.RavenRestoreInProgressDocumentKey, null);
            if (restoreDoc != null)
            {
                var restore = restoreDoc.DataAsJson.JsonDeserialization<RestoreInProgress>();
                resourceName = restore.Resource;
                return true;
            }
            return false;
        }

        protected bool HasPermissions(string path, out HttpResponseMessage message)
        {
            message = null;
            if (Directory.Exists(path) == false)
                return true;

            if (!HasPermissionsOnFolder(path))
            {
                {
                    message = GetMessageWithObject(new
                    {
                        Message = string.Format("No permissions for folder - {0}", path)
                    }, HttpStatusCode.BadRequest);
                    return false;
                }
            }
            return true;
        }

        private bool HasPermissionsOnFolder(string path)
        {
            try
            {
                var filename = Guid.NewGuid().ToString();
                var fullpath = Path.Combine(path, filename);
                using (var file = File.CreateText(fullpath))
                    file.Write(false);
                IOExtensions.DeleteFile(fullpath);
            }
            catch
            {
                return false;
            }
            return true;
        }
    }
}
