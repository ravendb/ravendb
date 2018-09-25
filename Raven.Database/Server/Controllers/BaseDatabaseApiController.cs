using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Security.Principal;
using System.Threading;

using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Indexing;
using Raven.Abstractions.Util;
using Raven.Bundles.Replication.Tasks;
using Raven.Database.Common;
using Raven.Database.Extensions;
using Raven.Database.Server.Abstractions;
using Raven.Database.Server.Security;
using Raven.Database.Server.Tenancy;
using Raven.Json.Linq;

namespace Raven.Database.Server.Controllers
{
    public abstract class BaseDatabaseApiController : ResourceApiController<DocumentDatabase, DatabasesLandlord>
    {
        public string DatabaseName
        {
            get
            {
                return ResourceName;
            }
        }

        public DocumentDatabase Database
        {
            get
            {
                return Resource;
            }
        }

        public override ResourceType ResourceType
        {
            get
            {
                return ResourceType.Database;
            }
        }

        public override void MarkRequestDuration(long duration)
        {
            if (Resource == null)
                return;
            Resource.WorkContext.MetricsCounters.RequestDurationMetric.Update(duration);
            Resource.WorkContext.MetricsCounters.RequestDurationLastMinute.AddRecord(duration);
        }

        private string queryFromPostRequest;

        public void SetPostRequestQuery(string query)
        {
            queryFromPostRequest = EscapingHelper.UnescapeLongDataString(query);
        }

        public void InitializeFrom(BaseDatabaseApiController other)
        {
            ResourceName = other.ResourceName;
            queryFromPostRequest = other.queryFromPostRequest;
            if (other.Configuration != null)
                Configuration = other.Configuration;
            ControllerContext = other.ControllerContext;
            ActionContext = other.ActionContext;
        }

        public override HttpResponseMessage GetEmptyMessage(HttpStatusCode code = HttpStatusCode.OK, Etag etag = null)
        {
            var result = base.GetEmptyMessage(code, etag);
            RequestManager.AddAccessControlHeaders(this, result);
            HandleReplication(result);
            return result;
        }

        public override HttpResponseMessage GetMessageWithObject(object item, HttpStatusCode code = HttpStatusCode.OK, Etag etag = null)
        {
            var result = base.GetMessageWithObject(item, code, etag);

            RequestManager.AddAccessControlHeaders(this, result);
            HandleReplication(result);
            return result;
        }

        public override HttpResponseMessage GetMessageWithString(string msg, HttpStatusCode code = HttpStatusCode.OK, Etag etag = null)
        {
            var result = base.GetMessageWithString(msg, code, etag);
            RequestManager.AddAccessControlHeaders(this, result);
            HandleReplication(result);
            return result;
        }

        protected TransactionInformation GetRequestTransaction()
        {
            if (InnerRequest.Headers.Contains("Raven-Transaction-Information") == false)
                return null;
            var txInfo = InnerRequest.Headers.GetValues("Raven-Transaction-Information").FirstOrDefault();
            if (string.IsNullOrEmpty(txInfo))
                return null;
            var parts = txInfo.Split(new[] { ", " }, StringSplitOptions.RemoveEmptyEntries);
            if (parts.Length != 2)
                throw new ArgumentException("'Raven-Transaction-Information' is in invalid format, expected format is: 'xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx, hh:mm:ss'");

            return new TransactionInformation
            {
                Id = parts[0],
                Timeout = TimeSpan.ParseExact(parts[1], "c", CultureInfo.InvariantCulture)
            };
        }

        protected virtual IndexQuery GetIndexQuery(int maxPageSize)
        {
            var query = new IndexQuery
            {
                Query = GetQueryStringValue("query") ?? queryFromPostRequest ?? "",
                Start = GetStart(),
                Cutoff = GetCutOff(),
                WaitForNonStaleResultsAsOfNow = GetWaitForNonStaleResultsAsOfNow(),
                CutoffEtag = GetCutOffEtag(),
                PageSize = GetPageSize(maxPageSize),
                FieldsToFetch = GetQueryStringValues("fetch"),
                DefaultField = GetQueryStringValue("defaultField"),

                DefaultOperator =
                    string.Equals(GetQueryStringValue("operator"), "AND", StringComparison.OrdinalIgnoreCase) ?
                        QueryOperator.And :
                        QueryOperator.Or,

                SortedFields = EnumerableExtension.EmptyIfNull(GetQueryStringValues("sort"))
                    .Select(x => new SortedField(x))
                    .ToArray(),
                HighlightedFields = GetHighlightedFields().ToArray(),
                HighlighterPreTags = GetQueryStringValues("preTags"),
                HighlighterPostTags = GetQueryStringValues("postTags"),
                HighlighterKeyName = GetQueryStringValue("highlighterKeyName"),
                ResultsTransformer = GetQueryStringValue("resultsTransformer"),
                TransformerParameters = ExtractTransformerParameters(),
                ExplainScores = GetExplainScores(),
                SortHints = GetSortHints(),
                IsDistinct = IsDistinct()
            };

            var allowMultipleIndexEntriesForSameDocumentToResultTransformer = GetQueryStringValue("allowMultipleIndexEntriesForSameDocumentToResultTransformer");
            bool allowMultiple;
            if (string.IsNullOrEmpty(allowMultipleIndexEntriesForSameDocumentToResultTransformer) == false && bool.TryParse(allowMultipleIndexEntriesForSameDocumentToResultTransformer, out allowMultiple))
                query.AllowMultipleIndexEntriesForSameDocumentToResultTransformer = allowMultiple;

            if (query.WaitForNonStaleResultsAsOfNow)
                query.Cutoff = SystemTime.UtcNow;

            var showTimingsAsString = GetQueryStringValue("showTimings");
            bool showTimings;
            if (string.IsNullOrEmpty(showTimingsAsString) == false && bool.TryParse(showTimingsAsString, out showTimings) && showTimings)
                query.ShowTimings = true;

            var skipDuplicateCheckingAsString = GetQueryStringValue("skipDuplicateChecking");
            bool skipDuplicateChecking;
            if (string.IsNullOrEmpty(skipDuplicateCheckingAsString) == false &&
                bool.TryParse(skipDuplicateCheckingAsString, out skipDuplicateChecking) && skipDuplicateChecking)
                query.SkipDuplicateChecking = true;

            var spatialFieldName = GetQueryStringValue("spatialField") ?? Constants.DefaultSpatialFieldName;
            var queryShape = GetQueryStringValue("queryShape");
            SpatialUnits units;
            var unitsSpecified = Enum.TryParse(GetQueryStringValue("spatialUnits"), out units);
            double distanceErrorPct;
            if (!double.TryParse(GetQueryStringValue("distErrPrc"), NumberStyles.Any, CultureInfo.InvariantCulture, out distanceErrorPct))
                distanceErrorPct = Constants.DefaultSpatialDistanceErrorPct;
            SpatialRelation spatialRelation;

            if (Enum.TryParse(GetQueryStringValue("spatialRelation"), false, out spatialRelation) && !string.IsNullOrWhiteSpace(queryShape))
            {
                return new SpatialIndexQuery(query)
                {
                    SpatialFieldName = spatialFieldName,
                    QueryShape = queryShape,
                    RadiusUnitOverride = unitsSpecified ? units : (SpatialUnits?)null,
                    SpatialRelation = spatialRelation,
                    DistanceErrorPercentage = distanceErrorPct,
                };
            }

            return query;
        }

        private bool IsDistinct()
        {
            var distinct = GetQueryStringValue("distinct");
            if (string.Equals("true", distinct, StringComparison.OrdinalIgnoreCase))
                return true;
            var aggAsString = GetQueryStringValue("aggregation"); // 2.x legacy support
            if (aggAsString == null)
                return false;

            if (string.Equals("Distinct", aggAsString, StringComparison.OrdinalIgnoreCase))
                return true;

            if (string.Equals("None", aggAsString, StringComparison.OrdinalIgnoreCase))
                return false;

            throw new NotSupportedException("AggregationOperation (except Distinct) is no longer supported");
        }

        private Dictionary<string, SortOptions> GetSortHints()
        {
            var result = new Dictionary<string, SortOptions>();

            // backward compatibility
            foreach (var header in InnerRequest.Headers.Where(pair => pair.Key.StartsWith("SortHint-")))
            {
                SortOptions sort;
                Enum.TryParse(GetHeader(header.Key), true, out sort);
                result[Uri.UnescapeDataString(header.Key)] = sort;
            }

            foreach (var pair in InnerRequest.GetQueryNameValuePairs().Where(pair => pair.Key.StartsWith("SortHint-", StringComparison.OrdinalIgnoreCase)))
            {
                var key = pair.Key;
                var value = pair.Value != null ? Uri.UnescapeDataString(pair.Value) : null;

                SortOptions sort;
                Enum.TryParse(value, true, out sort);
                result[Uri.UnescapeDataString(key)] = sort;
            }

            return result;
        }

        public Etag GetCutOffEtag()
        {
            var etagAsString = GetQueryStringValue("cutOffEtag");
            if (etagAsString != null)
            {
                etagAsString = Uri.UnescapeDataString(etagAsString);

                return Etag.Parse(etagAsString);
            }

            return null;
        }

        private bool GetExplainScores()
        {
            bool result;
            bool.TryParse(GetQueryStringValue("explainScores"), out result);
            return result;
        }

        private bool GetWaitForNonStaleResultsAsOfNow()
        {
            bool result;
            bool.TryParse(GetQueryStringValue("waitForNonStaleResultsAsOfNow"), out result);
            return result;
        }

        public DateTime? GetCutOff()
        {
            var etagAsString = GetQueryStringValue("cutOff");
            if (etagAsString != null)
            {
                etagAsString = Uri.UnescapeDataString(etagAsString);

                DateTime result;
                if (DateTime.TryParseExact(etagAsString, "o", CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out result))
                    return result;
                throw new BadRequestException("Could not parse cut off query parameter as date");
            }

            return null;
        }

        public IEnumerable<HighlightedField> GetHighlightedFields()
        {
            var highlightedFieldStrings = EnumerableExtension.EmptyIfNull(GetQueryStringValues("highlight"));
            var fields = new HashSet<string>();

            foreach (var highlightedFieldString in highlightedFieldStrings)
            {
                HighlightedField highlightedField;
                if (HighlightedField.TryParse(highlightedFieldString, out highlightedField))
                {
                    if (!fields.Add(highlightedField.Field))
                        throw new BadRequestException("Duplicate highlighted field has found: " + highlightedField.Field);

                    yield return highlightedField;
                }
                else
                    throw new BadRequestException("Could not parse highlight query parameter as field highlight options");
            }
        }

        public Dictionary<string, RavenJToken> ExtractTransformerParameters()
        {
            var result = new Dictionary<string, RavenJToken>();
            foreach (var key in InnerRequest.GetQueryNameValuePairs().Select(pair => pair.Key))
            {
                if (string.IsNullOrEmpty(key)) continue;
                if (key.StartsWith("qp-") || key.StartsWith("tp-"))
                {
                    var realkey = key.Substring(3);
                    result[realkey] = GetQueryStringValue(key);
                }
            }

            return result;
        }

        protected bool GetOverwriteExisting()
        {
            bool result;
            if (!bool.TryParse(GetQueryStringValue("overwriteExisting"), out result))
            {
                // Check legacy key.
                bool.TryParse(GetQueryStringValue("checkForUpdates"), out result);
            }

            return result;
        }

        protected bool GetCheckReferencesInIndexes()
        {
            bool result;
            bool.TryParse(GetQueryStringValue("checkReferencesInIndexes"), out result);
            return result;
        }

        protected bool GetAllowStale()
        {
            bool stale;
            bool.TryParse(GetQueryStringValue("allowStale"), out stale);
            return stale;
        }

        protected bool GetSkipOverwriteIfUnchanged()
        {
            bool result;
            bool.TryParse(GetQueryStringValue("skipOverwriteIfUnchanged"), out result);
            return result;
        }

        protected BulkInsertCompression GetCompression()
        {
            var compression = GetQueryStringValue("compression");
            if (string.IsNullOrWhiteSpace(compression))
                return BulkInsertCompression.GZip;

            switch (compression.ToLowerInvariant())
            {
                case "none": return BulkInsertCompression.None;
                case "gzip": return BulkInsertCompression.GZip;
                default: throw new NotSupportedException(string.Format("The compression algorithm '{0}' is not supported.", compression));
            }
        }

        protected BulkInsertFormat GetFormat()
        {
            var format = GetQueryStringValue("format");
            if (string.IsNullOrWhiteSpace(format))
                return BulkInsertFormat.Bson;

            switch (format.ToLowerInvariant())
            {
                case "bson": return BulkInsertFormat.Bson;
                case "json": return BulkInsertFormat.Json;
                default: throw new NotSupportedException(string.Format("The format '{0}' is not supported", format.ToString()));
            }
        }


        protected int? GetMaxOpsPerSec()
        {
            int? result = null;
            int parseResult;
            var success = int.TryParse(GetQueryStringValue("maxOpsPerSec"), out parseResult);
            if (success) result = parseResult;
            return result;
        }

        protected TimeSpan? GetStaleTimeout()
        {
            TimeSpan? result = null;
            TimeSpan parseResult;
            var success = TimeSpan.TryParse(GetQueryStringValue("staleTimeout"), out parseResult);
            if (success) result = parseResult;
            return result;
        }

        protected bool GetRetrieveDetails()
        {
            bool details;
            bool.TryParse(GetQueryStringValue("details"), out details);
            return details;
        }

        protected void HandleReplication(HttpResponseMessage msg)
        {

            if (msg.StatusCode == HttpStatusCode.BadRequest ||
                msg.StatusCode == HttpStatusCode.ServiceUnavailable ||
                msg.StatusCode == HttpStatusCode.InternalServerError
                )
                return;

            var clientPrimaryServerUrl = GetHeader(Constants.RavenClientPrimaryServerUrl);
            var clientPrimaryServerLastCheck = GetHeader(Constants.RavenClientPrimaryServerLastCheck);
            if (string.IsNullOrEmpty(clientPrimaryServerUrl) || string.IsNullOrEmpty(clientPrimaryServerLastCheck))
            {
                return;
            }

            DateTime primaryServerLastCheck;
            if (DateTime.TryParse(clientPrimaryServerLastCheck, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out primaryServerLastCheck) == false)
            {
                return;
            }

            var replicationTask = Resource.StartupTasks.OfType<ReplicationTask>().FirstOrDefault();
            if (replicationTask == null)
            {
                return;
            }

            if (replicationTask.IsHeartbeatAvailable(clientPrimaryServerUrl, primaryServerLastCheck))
            {
                msg.Headers.TryAddWithoutValidation(Constants.RavenForcePrimaryServerCheck, "True");
            }
        }

        protected Etag GetLastDocEtag()
        {
            var lastDocEtag = Etag.Empty;
            long documentsCount = 0;
            Resource.TransactionalStorage.Batch(
                accessor =>
                {
                    lastDocEtag = accessor.Staleness.GetMostRecentDocumentEtag();
                    documentsCount = accessor.Documents.GetDocumentsCount();
                });

            lastDocEtag = lastDocEtag.HashWith(BitConverter.GetBytes(documentsCount));
            return lastDocEtag;
        }

        protected class TenantData
        {
            public bool IsLoaded { get; set; }
            public string Name { get; set; }
            public bool Disabled { get; set; }
            public string[] Bundles { get; set; }
            public bool IsAdminCurrentTenant { get; set; }
        }

        protected HttpResponseMessage Resources<T>(string prefix, Func<RavenJArray, List<T>> getResourcesData, bool getAdditionalData = false)
            where T : TenantData
        {
            if (EnsureSystemDatabase() == false)
                return
                    GetMessageWithString(
                        "The request '" + InnerRequest.RequestUri.AbsoluteUri + "' can only be issued on the system database",
                        HttpStatusCode.BadRequest);

            // This method is NOT secured, and anyone can access it.
            // Because of that, we need to provide explicit security here.

            // Anonymous Access - All / Get / Admin
            // Show all resources

            // Anonymous Access - None
            // Show only the resource that you have access to (read / read-write / admin)

            // If admin, show all resources

            var resourcesDocuments = GetResourcesDocuments(prefix);
            var resourcesData = getResourcesData(resourcesDocuments);
            var resourcesNames = resourcesData.Select(resourceObject => resourceObject.Name).ToArray();

            List<string> approvedResources = null;
            if (SystemConfiguration.AnonymousUserAccessMode == AnonymousUserAccessMode.None)
            {
                var authorizer = (MixedModeRequestAuthorizer)ControllerContext.Configuration.Properties[typeof(MixedModeRequestAuthorizer)];

                HttpResponseMessage authMsg;
                if (authorizer.TryAuthorize(this, out authMsg) == false)
                    return authMsg;

                var user = authorizer.GetUser(this);
                if (user == null)
                    return authMsg;

                if (user.IsAdministrator(SystemConfiguration.AnonymousUserAccessMode) == false)
                {
                    approvedResources = authorizer.GetApprovedResources(user, this, resourcesNames);
                }

                resourcesData.ForEach(x =>
                {
                    var principalWithDatabaseAccess = user as PrincipalWithDatabaseAccess;
                    if (principalWithDatabaseAccess != null)
                    {
                        var isAdminGlobal = principalWithDatabaseAccess.IsAdministrator(SystemConfiguration.AnonymousUserAccessMode);
                        x.IsAdminCurrentTenant = isAdminGlobal || principalWithDatabaseAccess.IsAdministrator(Resource);
                    }
                    else
                    {
                        x.IsAdminCurrentTenant = user.IsAdministrator(x.Name);
                    }
                });
            }

            var lastDocEtag = GetLastDocEtag();
            if (MatchEtag(lastDocEtag))
                return GetEmptyMessage(HttpStatusCode.NotModified);

            if (approvedResources != null)
            {
                resourcesData = resourcesData.Where(data => approvedResources.Contains(data.Name)).ToList();
                resourcesNames = resourcesNames.Where(name => approvedResources.Contains(name)).ToArray();
            }

            var responseMessage = getAdditionalData ? GetMessageWithObject(resourcesData) : GetMessageWithObject(resourcesNames);
            WriteHeaders(new RavenJObject(), lastDocEtag, responseMessage);
            return responseMessage.WithNoCache();
        }

        protected RavenJArray GetResourcesDocuments(string resourcePrefix)
        {
            var start = GetStart();
            var nextPageStart = start; // will trigger rapid pagination
            var resourcesDocuments = Resource.Documents.GetDocumentsWithIdStartingWith(resourcePrefix, null, null, start,
                int.MaxValue, CancellationToken.None, ref nextPageStart);

            return resourcesDocuments;
        }

        protected UserInfo GetUserInfo()
        {
            var principal = User;
            var anonymousUserAccessMode = DatabasesLandlord.SystemConfiguration.AnonymousUserAccessMode;
            if (principal == null || principal.Identity == null || principal.Identity.IsAuthenticated == false)
            {
                var anonymous = new UserInfo
                {
                    Remark = "Using anonymous user",
                    IsAdminGlobal = anonymousUserAccessMode == AnonymousUserAccessMode.Admin,
                    IsAdminCurrentDb = anonymousUserAccessMode == AnonymousUserAccessMode.Admin ||
                                       anonymousUserAccessMode == AnonymousUserAccessMode.All
                };

                return anonymous;
            }

            var windowsPrincipal = principal as WindowsPrincipal;
            if (windowsPrincipal != null)
            {
                var windowsUser = new UserInfo
                {
                    Remark = "Using windows auth",
                    User = windowsPrincipal.Identity.Name,
                    IsAdminGlobal = windowsPrincipal.IsAdministrator("<system>") ||
                                    windowsPrincipal.IsAdministrator(anonymousUserAccessMode),
                    IsBackupOperator = windowsPrincipal.IsBackupOperator(anonymousUserAccessMode)
                };

                windowsUser.IsAdminCurrentDb = windowsUser.IsAdminGlobal || windowsPrincipal.IsAdministrator(Resource);

                return windowsUser;
            }

            var principalWithDatabaseAccess = principal as PrincipalWithDatabaseAccess;
            if (principalWithDatabaseAccess != null)
            {
                var windowsUserWithDatabase = new UserInfo
                {
                    Remark = "Using windows auth",
                    User = principalWithDatabaseAccess.Identity.Name,
                    IsAdminGlobal = principalWithDatabaseAccess.IsAdministrator("<system>") ||
                                    principalWithDatabaseAccess.IsAdministrator(anonymousUserAccessMode),
                    IsAdminCurrentDb = principalWithDatabaseAccess.IsAdministrator(Resource),
                    Databases =
                        principalWithDatabaseAccess.AdminDatabases.Concat(
                            principalWithDatabaseAccess.ReadOnlyDatabases)
                                                   .Concat(principalWithDatabaseAccess.ReadWriteDatabases)
                                                   .Select(db => new DatabaseInfo
                                                   {
                                                       Database = db,
                                                       IsAdmin = principal.IsAdministrator(db),
                                                       IsReadOnly = principal.IsReadOnly(db),
                                                   }).ToList(),

                    AdminDatabases = principalWithDatabaseAccess.AdminDatabases,
                    ReadOnlyDatabases = principalWithDatabaseAccess.ReadOnlyDatabases,
                    ReadWriteDatabases = principalWithDatabaseAccess.ReadWriteDatabases,
                    IsBackupOperator = principalWithDatabaseAccess.IsBackupOperator(anonymousUserAccessMode)
                };

                windowsUserWithDatabase.IsAdminCurrentDb = windowsUserWithDatabase.IsAdminGlobal || principalWithDatabaseAccess.IsAdministrator(Resource);

                return windowsUserWithDatabase;
            }

            var oAuthPrincipal = principal as OAuthPrincipal;
            if (oAuthPrincipal != null)
            {

                var oAuth = new UserInfo
                {
                    Remark = "Using OAuth",
                    User = oAuthPrincipal.Name,
                    IsAdminGlobal = oAuthPrincipal.IsAdministrator(anonymousUserAccessMode),
                    IsAdminCurrentDb = oAuthPrincipal.IsAdministrator(Resource),
                    Databases = oAuthPrincipal.TokenBody.AuthorizedDatabases
                                              .Select(db => db.TenantId != null ? new DatabaseInfo
                                              {
                                                  Database = db.TenantId,
                                                  IsAdmin = principal.IsAdministrator(db.TenantId),
                                                  IsReadOnly = db.ReadOnly

                                              } : null).ToList(),

                    AccessTokenBody = oAuthPrincipal.TokenBody,

                    AdminDatabases = oAuthPrincipal.AdminDatabases,
                    ReadOnlyDatabases = oAuthPrincipal.ReadOnlyDatabases,
                    ReadWriteDatabases = oAuthPrincipal.ReadWriteDatabases,
                    IsBackupOperator = oAuthPrincipal.IsBackupOperator(anonymousUserAccessMode)
                };

                return oAuth;
            }

            var unknown = new UserInfo
            {
                Remark = "Unknown auth",
                Principal = principal
            };

            return unknown;
        }

        protected bool CanExposeConfigOverTheWire()
        {
            if (string.Equals(SystemConfiguration.ExposeConfigOverTheWire, "AdminOnly", StringComparison.OrdinalIgnoreCase) && SystemConfiguration.AnonymousUserAccessMode != AnonymousUserAccessMode.Admin)
            {
                var authorizer = (MixedModeRequestAuthorizer)ControllerContext.Configuration.Properties[typeof(MixedModeRequestAuthorizer)];
                var user = authorizer.GetUser(this);
                if (user == null || user.IsAdministrator(SystemConfiguration.AnonymousUserAccessMode) == false)
                {
                    return false;
                }
            }

            return true;
        }
    }
}