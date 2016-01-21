using System;
using System.Net;
using System.Runtime.Serialization;
using System.Security.Principal;
using System.Threading.Tasks;
using System.Web;

using Raven.Abstractions.Data;
using Raven.Database.Server.Abstractions;
using Raven.Database.Server.Security;
using Raven.Database.Server.Tenancy;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;

namespace Raven.Database.Server.Connections
{
    public class WebSocketsRequestParser
    {
        private const string CountersUrlPrefix = Constants.Counter.UrlPrefix;
        private const string DatabasesUrlPrefix = Constants.Database.UrlPrefix;
        private const string FileSystemsUrlPrefix = Constants.FileSystem.UrlPrefix;

        protected DatabasesLandlord DatabasesLandlord { get; private set; }

        private readonly CountersLandlord countersLandlord;

        private readonly FileSystemsLandlord fileSystemsLandlord;

        private readonly MixedModeRequestAuthorizer authorizer;

        private readonly string expectedRequestSuffix;

        public WebSocketsRequestParser(DatabasesLandlord databasesLandlord, CountersLandlord countersLandlord, FileSystemsLandlord fileSystemsLandlord, MixedModeRequestAuthorizer authorizer, string expectedRequestSuffix)
        {
            DatabasesLandlord = databasesLandlord;
            this.countersLandlord = countersLandlord;
            this.fileSystemsLandlord = fileSystemsLandlord;
            this.authorizer = authorizer;
            this.expectedRequestSuffix = expectedRequestSuffix;
        }

        public async Task<WebSocketRequest> ParseWebSocketRequestAsync(Uri uri, string token)
        {
            var parameters = HttpUtility.ParseQueryString(uri.Query);
            var request = new WebSocketRequest
            {
                Id = parameters["id"],
                Uri = uri,
                Token = token
            };

            await ValidateRequest(request);
            AuthenticateRequest(request);

            return request;
        }

        protected virtual async Task ValidateRequest(WebSocketRequest request)
        {
            if (string.IsNullOrEmpty(request.Id))
            {
                throw new WebSocketRequestValidationException(HttpStatusCode.BadRequest, "Id is mandatory.");
            }

            request.ActiveResource = await GetActiveResource(request);
            request.ResourceName = request.ActiveResource.ResourceName ?? Constants.SystemDatabase;
        }

        protected virtual void AuthenticateRequest(WebSocketRequest request)
        {
            var singleUseToken = request.Token;
            if (string.IsNullOrEmpty(singleUseToken) == false)
            {
                object msg;
                HttpStatusCode code;

                IPrincipal user;
                if (authorizer.TryAuthorizeSingleUseAuthToken(singleUseToken, request.ResourceName, out msg, out code, out user) == false)
                {
                    throw new WebSocketRequestValidationException(code, RavenJToken.FromObject(msg).ToString(Formatting.Indented));
                }

                request.User = user;
                return;
            }

            switch (DatabasesLandlord.SystemDatabase.Configuration.AnonymousUserAccessMode)
            {
                case AnonymousUserAccessMode.Admin:
                case AnonymousUserAccessMode.All:
                case AnonymousUserAccessMode.Get:
                    // this is effectively a GET request, so we'll allow it
                    // under this circumstances
                    request.User = CurrentOperationContext.User.Value;
                    return;
                case AnonymousUserAccessMode.None:
                    throw new WebSocketRequestValidationException(HttpStatusCode.Forbidden, "Single use token is required for authenticated web sockets connections.");
                default:
                    throw new ArgumentOutOfRangeException(DatabasesLandlord.SystemDatabase.Configuration.AnonymousUserAccessMode.ToString());
            }
        }

        private async Task<IResourceStore> GetActiveResource(WebSocketRequest request)
        {
            try
            {
                var localPath = NormalizeLocalPath(request.Uri.LocalPath);
                var resourcePath = localPath.Substring(0, localPath.Length - expectedRequestSuffix.Length);

                var resourcePartsPathParts = resourcePath.Split('/');

                if (expectedRequestSuffix.Equals(localPath))
                {
                    return DatabasesLandlord.SystemDatabase;
                }
                IResourceStore activeResource;
                switch (resourcePartsPathParts[1])
                {
                    case CountersUrlPrefix:
                        activeResource = await countersLandlord.GetCounterInternal(resourcePath.Substring(CountersUrlPrefix.Length + 2));
                        break;
                    case DatabasesUrlPrefix:
                        activeResource = await DatabasesLandlord.GetDatabaseInternal(resourcePath.Substring(DatabasesUrlPrefix.Length + 2));
                        break;
                    case FileSystemsUrlPrefix:
                        activeResource = await fileSystemsLandlord.GetFileSystemInternalAsync(resourcePath.Substring(FileSystemsUrlPrefix.Length + 2));
                        break;
                    default:
                        throw new WebSocketRequestValidationException(HttpStatusCode.BadRequest, "Illegal websocket path.");
                }

                return activeResource;
            }
            catch (Exception e)
            {
                throw new WebSocketRequestValidationException(HttpStatusCode.InternalServerError, e.Message);
            }
        }

        private string NormalizeLocalPath(string localPath)
        {
            if (string.IsNullOrEmpty(localPath))
                return null;

            if (localPath.StartsWith(DatabasesLandlord.SystemDatabase.Configuration.VirtualDirectory))
                localPath = localPath.Substring(DatabasesLandlord.SystemDatabase.Configuration.VirtualDirectory.Length);

            if (localPath.StartsWith("/") == false)
                localPath = "/" + localPath;

            return localPath;
        }

        [Serializable]
        public class  WebSocketRequestValidationException : Exception
        {
            public HttpStatusCode StatusCode { get; set; }

            public  WebSocketRequestValidationException()
            {
            }

            public  WebSocketRequestValidationException(HttpStatusCode statusCode, string message) : base(message)
            {
                StatusCode = statusCode;
            }

            public  WebSocketRequestValidationException(string message, Exception inner) : base(message, inner)
            {
            }

            protected  WebSocketRequestValidationException(
                SerializationInfo info,
                StreamingContext context) : base(info, context)
            {
            }
        }
    }

    public class WatchTrafficWebSocketsRequestParser : WebSocketsRequestParser
    {
        public WatchTrafficWebSocketsRequestParser(DatabasesLandlord databasesLandlord, CountersLandlord countersLandlord, FileSystemsLandlord fileSystemsLandlord, MixedModeRequestAuthorizer authorizer, string expectedRequestSuffix)
            : base(databasesLandlord, countersLandlord, fileSystemsLandlord, authorizer, expectedRequestSuffix)
        {
        }

        protected override void AuthenticateRequest(WebSocketRequest request)
        {
            base.AuthenticateRequest(request);

            if (request.ResourceName == Constants.SystemDatabase)
            {
                var oneTimetokenPrincipal = request.User as OneTimeTokenPrincipal;

                if ((oneTimetokenPrincipal == null || !oneTimetokenPrincipal.IsAdministratorInAnonymouseMode) &&
                    DatabasesLandlord.SystemDatabase.Configuration.AnonymousUserAccessMode != AnonymousUserAccessMode.Admin)
                {
                    throw new WebSocketRequestValidationException(HttpStatusCode.Forbidden, "Administrator user is required in order to trace the whole server.");
                }
            }
        }
    }

    public class AdminLogsWebSocketsRequestParser : WebSocketsRequestParser
    {
        public AdminLogsWebSocketsRequestParser(DatabasesLandlord databasesLandlord, CountersLandlord countersLandlord, FileSystemsLandlord fileSystemsLandlord, MixedModeRequestAuthorizer authorizer, string expectedRequestSuffix)
            : base(databasesLandlord, countersLandlord, fileSystemsLandlord, authorizer, expectedRequestSuffix)
        {
        }

        protected override async Task ValidateRequest(WebSocketRequest request)
        {
            await base.ValidateRequest(request);

            if (request.ResourceName != Constants.SystemDatabase)
                throw new WebSocketRequestValidationException(HttpStatusCode.BadRequest, "Request should be without resource context, or with system database.");
        }

        protected override void AuthenticateRequest(WebSocketRequest request)
        {
            base.AuthenticateRequest(request);

            var oneTimetokenPrincipal = request.User as OneTimeTokenPrincipal;

            if ((oneTimetokenPrincipal == null || !oneTimetokenPrincipal.IsAdministratorInAnonymouseMode) &&
                DatabasesLandlord.SystemDatabase.Configuration.AnonymousUserAccessMode != AnonymousUserAccessMode.Admin)
            {
                throw new WebSocketRequestValidationException(HttpStatusCode.BadRequest, "Administrator user is required in order to trace the whole server.");
            }
        }
    }
}
