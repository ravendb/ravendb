using System.Security.Principal;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Indexing;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Replication;
using Raven.Abstractions.Util;
using Raven.Bundles.Replication.Tasks;
using Raven.Database.Config;
using Raven.Database.Config.Retriever;
using Raven.Database.Extensions;
using Raven.Database.Server.Abstractions;
using Raven.Database.Server.Security;
using Raven.Database.Server.WebApi;
using Raven.Json.Linq;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using System.Web;
using System.Web.Http.Controllers;
using System.Web.Http.Routing;

namespace Raven.Database.Server.Controllers
{
	public abstract class RavenDbApiController : RavenBaseApiController
	{
	    private static readonly ILog Logger = LogManager.GetCurrentClassLogger();

		public string DatabaseName { get; private set; }

		private string queryFromPostRequest;
		
		public void SetPostRequestQuery(string query)
		{
			queryFromPostRequest = EscapingHelper.UnescapeLongDataString(query);
		}

		public void InitializeFrom(RavenDbApiController other)
		{
			DatabaseName = other.DatabaseName;
			queryFromPostRequest = other.queryFromPostRequest;
			Configuration = other.Configuration;
			ControllerContext = other.ControllerContext;
			ActionContext = other.ActionContext;
		}

		public override async Task<HttpResponseMessage> ExecuteAsync(HttpControllerContext controllerContext, CancellationToken cancellationToken)
		{
			InnerInitialization(controllerContext);
			var authorizer = (MixedModeRequestAuthorizer)controllerContext.Configuration.Properties[typeof(MixedModeRequestAuthorizer)];
			var result = new HttpResponseMessage();
			if (InnerRequest.Method.Method != "OPTIONS")
			{
				result = await RequestManager.HandleActualRequest(this, controllerContext, async () =>
				{
                    RequestManager.SetThreadLocalState(ReadInnerHeaders, DatabaseName);
					return await ExecuteActualRequest(controllerContext, cancellationToken, authorizer);
				}, httpException =>
				{
				    var response = GetMessageWithObject(new { Error = httpException.Message }, HttpStatusCode.ServiceUnavailable);

				    var timeout = httpException.InnerException as TimeoutException;
                    if (timeout != null)
                    {
                        response.Headers.Add("Raven-Database-Load-In-Progress", DatabaseName);
                    }
				    return response;
				});
			}

			RequestManager.AddAccessControlHeaders(this, result);
            RequestManager.ResetThreadLocalState();

			return result;
		}

		private async Task<HttpResponseMessage> ExecuteActualRequest(HttpControllerContext controllerContext, CancellationToken cancellationToken,
			MixedModeRequestAuthorizer authorizer)
		{
			if (SkipAuthorizationSinceThisIsMultiGetRequestAlreadyAuthorized == false)
			{
				HttpResponseMessage authMsg;
				if (authorizer.TryAuthorize(this, out authMsg) == false)
					return authMsg;
			}

            if (IsInternalRequest == false)
				RequestManager.IncrementRequestCount();

			if (DatabaseName != null && await DatabasesLandlord.GetDatabaseInternal(DatabaseName) == null)
			{
				var msg = "Could not find a database named: " + DatabaseName;
				return GetMessageWithObject(new { Error = msg }, HttpStatusCode.ServiceUnavailable);
			}

			var sp = Stopwatch.StartNew();

			var result = await base.ExecuteAsync(controllerContext, cancellationToken);
			sp.Stop();
			AddRavenHeader(result, sp);

			return result;
		}

		protected ReplicationDocument<ReplicationDestination.ReplicationDestinationWithConfigurationOrigin> GetReplicationDocument(out HttpResponseMessage erroResponseMessage)
		{
			ConfigurationDocument<ReplicationDocument<ReplicationDestination.ReplicationDestinationWithConfigurationOrigin>> configurationDocument;
			erroResponseMessage = null;
			try
			{
				configurationDocument = Database.ConfigurationRetriever.GetConfigurationDocument<ReplicationDocument<ReplicationDestination.ReplicationDestinationWithConfigurationOrigin>>(Constants.RavenReplicationDestinations);
			}
			catch (Exception e)
			{
				const string errorMessage = "Something very wrong has happened, was unable to retrieve replication destinations.";
				Log.ErrorException(errorMessage, e);
				erroResponseMessage = GetMessageWithObject(new { Message = errorMessage + " Check server logs for more details." }, HttpStatusCode.InternalServerError);
				return null;
			}

			if (configurationDocument == null)
			{
				erroResponseMessage = GetMessageWithObject(new { Message = "Replication destinations not found. Perhaps no replication is configured? Nothing to do in this case..." }, HttpStatusCode.NotFound);
				return null;
			}

			if (configurationDocument.MergedDocument.Destinations.Count != 0) 
				return configurationDocument.MergedDocument;

			erroResponseMessage = GetMessageWithObject(new
			{
				Message = @"Replication document found, but no destinations configured for index replication. 
																Maybe all replication destinations have SkipIndexReplication flag equals to true?  
																Nothing to do in this case..."
			},
				HttpStatusCode.NoContent);
			return null;
		}

		protected override void InnerInitialization(HttpControllerContext controllerContext)
		{
			base.InnerInitialization(controllerContext);

			var values = controllerContext.Request.GetRouteData().Values;
			if (values.ContainsKey("MS_SubRoutes"))
			{
				var routeDatas = (IHttpRouteData[])controllerContext.Request.GetRouteData().Values["MS_SubRoutes"];
				var selectedData = routeDatas.FirstOrDefault(data => data.Values.ContainsKey("databaseName"));

				if (selectedData != null)
					DatabaseName = selectedData.Values["databaseName"] as string;
				else
					DatabaseName = null;
			}
			else
			{
				if (values.ContainsKey("databaseName"))
					DatabaseName = values["databaseName"] as string;
				else
					DatabaseName = null;
			}
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
			var result =base.GetMessageWithString(msg, code, etag);
			RequestManager.AddAccessControlHeaders(this, result);
			HandleReplication(result);
			return result;
		}

        public override InMemoryRavenConfiguration SystemConfiguration
        {
            get { return DatabasesLandlord.SystemConfiguration; }
        }

	    private DocumentDatabase _currentDb;
		public DocumentDatabase Database
		{
			get
			{
			    if (_currentDb != null)
			        return _currentDb;

				var database = DatabasesLandlord.GetDatabaseInternal(DatabaseName);
				if (database == null)
				{
					throw new InvalidOperationException("Could not find a database named: " + DatabaseName);
				}

                return _currentDb = database.Result;
			}
		}

	    public override InMemoryRavenConfiguration ResourceConfiguration
	    {
	        get { return Database.Configuration; }
	    }


	    protected bool EnsureSystemDatabase()
		{
			return DatabasesLandlord.SystemDatabase == Database;
		}

        protected bool IsAnotherRestoreInProgress(out string resourceName)
        {
            resourceName = null;
            var restoreDoc = Database.Documents.Get(RestoreInProgress.RavenRestoreInProgressDocumentKey, null);
            if (restoreDoc != null)
            {
                var restore = restoreDoc.DataAsJson.JsonDeserialization<RestoreInProgress>();
                resourceName = restore.Resource;
                return true;
            }
            return false;
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

			var replicationTask = Database.StartupTasks.OfType<ReplicationTask>().FirstOrDefault();
			if (replicationTask == null)
			{
				return;
			}

			if (replicationTask.IsHeartbeatAvailable(clientPrimaryServerUrl, primaryServerLastCheck))
			{
				msg.Headers.TryAddWithoutValidation(Constants.RavenForcePrimaryServerCheck, "True");
			}
		}


        public override bool SetupRequestToProperDatabase(RequestManager rm)
        {
            var tenantId = this.DatabaseName;
            var landlord = this.DatabasesLandlord;

            if (string.IsNullOrWhiteSpace(tenantId) || tenantId == "<system>")
            {                
                landlord.LastRecentlyUsed.AddOrUpdate("System", SystemTime.UtcNow, (s, time) => SystemTime.UtcNow);

                var args = new BeforeRequestWebApiEventArgs
                {
                    Controller = this,
                    IgnoreRequest = false,
                    TenantId = "System",
                    Database = landlord.SystemDatabase
                };

                rm.OnBeforeRequest(args);
                if (args.IgnoreRequest)
                    return false;
                return true;
            }

            Task<DocumentDatabase> resourceStoreTask;
            bool hasDb;
            try
            {
                hasDb = landlord.TryGetOrCreateResourceStore(tenantId, out resourceStoreTask);
            }
            catch (Exception e)
            {
                var msg = "Could not open database named: " + tenantId + " "  + e.Message;
                Logger.WarnException(msg, e);
                throw new HttpException(503, msg, e);
            }
            if (hasDb)
            {
                try
                {
					int TimeToWaitForDatabaseToLoad = MaxSecondsForTaskToWaitForDatabaseToLoad;
					if (resourceStoreTask.IsCompleted == false && resourceStoreTask.IsFaulted == false)
					{
						if (MaxNumberOfThreadsForDatabaseToLoad.Wait(0) == false)
						{
							var msg = string.Format("The database {0} is currently being loaded, but there are too many requests waiting for database load. Please try again later, database loading continues.", tenantId);
							Logger.Warn(msg);
							throw new TimeoutException(msg);
						}

						try
						{
							if (resourceStoreTask.Wait(TimeSpan.FromSeconds(TimeToWaitForDatabaseToLoad)) == false)
							{
								var msg = string.Format("The database {0} is currently being loaded, but after {1} seconds, this request has been aborted. Please try again later, database loading continues.", tenantId, TimeToWaitForDatabaseToLoad);
								Logger.Warn(msg);
								throw new TimeoutException(msg);
							}
						}
						finally
						{
							MaxNumberOfThreadsForDatabaseToLoad.Release();
						}
					}

                    var args = new BeforeRequestWebApiEventArgs()
                    {
                        Controller = this,
                        IgnoreRequest = false,
                        TenantId = tenantId,
                        Database = resourceStoreTask.Result
                    };

                    rm.OnBeforeRequest(args);
                    if (args.IgnoreRequest)
                        return false;
                }
                catch (Exception e)
                {
	                var msg = "Could not open database named: " + tenantId + Environment.NewLine + e;

                    Logger.WarnException(msg, e);
                    throw new HttpException(503, msg,e);
                }

                landlord.LastRecentlyUsed.AddOrUpdate(tenantId, SystemTime.UtcNow, (s, time) => SystemTime.UtcNow);
            }
            else
            {
                var msg = "Could not find a database named: " + tenantId;
                Logger.Warn(msg);
                throw new HttpException(503, msg);
            }
            return true;
        }

	    public override string TenantName
	    {
            get { return DatabaseName; }
	    }

	    public override void MarkRequestDuration(long duration)
	    {
	        if (Database == null)
	            return;
	        Database.WorkContext.MetricsCounters.RequestDuationMetric.Update(duration);
	    }

	    public bool ClientIsV3OrHigher
	    {
	        get
	        {
	            IEnumerable<string> values;
	            if (Request.Headers.TryGetValues("Raven-Client-Version", out values) == false)
                    return false; // probably 1.0 client?

	            return values.All(x => string.IsNullOrEmpty(x) == false && (x[0] != '1' && x[0] != '2'));
	        }
	    }

		protected class TenantData
		{
			public string Name { get; set; }
			public bool Disabled { get; set; }
			public string[] Bundles { get; set; }
			public bool IsAdminCurrentTenant { get; set; }
		}

		protected UserInfo GetUserInfo()
		{
			var principal = User;
			if (principal == null || principal.Identity == null || principal.Identity.IsAuthenticated == false)
			{
				var anonymous = new UserInfo
				{
					Remark = "Using anonymous user",
					IsAdminGlobal = DatabasesLandlord.SystemConfiguration.AnonymousUserAccessMode == AnonymousUserAccessMode.Admin
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
					IsAdminGlobal = windowsPrincipal.IsAdministrator(DatabasesLandlord.SystemConfiguration.AnonymousUserAccessMode)
				};

				windowsUser.IsAdminCurrentDb = windowsUser.IsAdminGlobal || windowsPrincipal.IsAdministrator(Database);

				return windowsUser;
			}

			var principalWithDatabaseAccess = principal as PrincipalWithDatabaseAccess;
			if (principalWithDatabaseAccess != null)
			{
				var windowsUserWithDatabase = new UserInfo
				{
					Remark = "Using windows auth",
					User = principalWithDatabaseAccess.Identity.Name,
					IsAdminGlobal =
						principalWithDatabaseAccess.IsAdministrator(
							DatabasesLandlord.SystemConfiguration.AnonymousUserAccessMode),
					IsAdminCurrentDb = principalWithDatabaseAccess.IsAdministrator(Database),
					Databases =
						principalWithDatabaseAccess.AdminDatabases.Concat(
							principalWithDatabaseAccess.ReadOnlyDatabases)
												   .Concat(principalWithDatabaseAccess.ReadWriteDatabases)
												   .Select(db => new DatabaseInfo
												   {
													   Database = db,
													   IsAdmin = principal.IsAdministrator(db)
												   }).ToList(),
					AdminDatabases = principalWithDatabaseAccess.AdminDatabases,
					ReadOnlyDatabases = principalWithDatabaseAccess.ReadOnlyDatabases,
					ReadWriteDatabases = principalWithDatabaseAccess.ReadWriteDatabases
				};

				windowsUserWithDatabase.IsAdminCurrentDb = windowsUserWithDatabase.IsAdminGlobal || principalWithDatabaseAccess.IsAdministrator(Database);

				return windowsUserWithDatabase;
			}

			var oAuthPrincipal = principal as OAuthPrincipal;
			if (oAuthPrincipal != null)
			{
				var oAuth = new UserInfo
				{
					Remark = "Using OAuth",
					User = oAuthPrincipal.Name,
					IsAdminGlobal = oAuthPrincipal.IsAdministrator(DatabasesLandlord.SystemConfiguration.AnonymousUserAccessMode),
					IsAdminCurrentDb = oAuthPrincipal.IsAdministrator(Database),
					Databases = oAuthPrincipal.TokenBody.AuthorizedDatabases
											  .Select(db => new DatabaseInfo
											  {
												  Database = db.TenantId,
												  IsAdmin = principal.IsAdministrator(db.TenantId)
											  }).ToList(),
					AccessTokenBody = oAuthPrincipal.TokenBody,
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
			if (SystemConfiguration.ExposeConfigOverTheWire == "AdminOnly" && SystemConfiguration.AnonymousUserAccessMode == AnonymousUserAccessMode.None)
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