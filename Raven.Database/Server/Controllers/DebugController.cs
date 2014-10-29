using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Security.Principal;
using System.Threading.Tasks;
using System.Web.Http;
using System.Web.Http.Controllers;
using System.Web.Http.Routing;
using ICSharpCode.NRefactory.CSharp;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Util;
using Raven.Database.Bundles.SqlReplication;
using Raven.Database.Extensions;
using Raven.Database.Linq;
using Raven.Database.Linq.Ast;
using Raven.Database.Linq.PrivateExtensions;
using Raven.Database.Server.Abstractions;
using Raven.Database.Storage;
using Raven.Database.Util;
using IOExtensions = Raven.Database.Extensions.IOExtensions;

namespace Raven.Database.Server.Controllers
{
	[RoutePrefix("")]
	public class DebugController : RavenDbApiController
	{
		[HttpGet]
		[Route("debug/prefetch-status")]
		[Route("databases/{databaseName}/debug/prefetch-status")]
		public HttpResponseMessage PrefetchingQueueStatus()
		{
			return GetMessageWithObject(DebugInfoProvider.GetPrefetchingQueueStatusForDebug(Database));
		}

		[HttpPost]
		[Route("debug/format-index")]
		[Route("databases/{databaseName}/debug/format-index")]
		public async Task<HttpResponseMessage> FormatIndex()
		{
			var array = await ReadJsonArrayAsync();
			var results = new string[array.Length];
			for (int i = 0; i < array.Length; i++)
			{
				var value = array[i].Value<string>();
				try
				{
					results[i] = IndexPrettyPrinter.Format(value);
				}
				catch (Exception e)
				{
					results[i] = "Could not format:" + Environment.NewLine +
								 value + Environment.NewLine + e;
				}
			}

			return GetMessageWithObject(results);
		}

        /// <summary>
        /// 
        /// </summary>
        /// <remarks>
        /// as we sum data we have to guarantee that we don't sum the same record twice on client side.
        /// to prevent such situation we don't send data from current second
        /// </remarks>
        /// <param name="format"></param>
        /// <returns></returns>
		[HttpGet]
		[Route("debug/indexing-perf-stats")]
		[Route("databases/{databaseName}/debug/indexing-perf-stats")]
		public HttpResponseMessage IndexingPerfStats(string format = "json")
        {
            var now = SystemTime.UtcNow;
            var nowTruncToSeconds = new DateTime(now.Ticks / TimeSpan.TicksPerSecond * TimeSpan.TicksPerSecond, now.Kind);

			var databaseStatistics = Database.Statistics;
			var stats = from index in databaseStatistics.Indexes
						from perf in index.Performance
                        where (perf.Operation == "Map" || perf.Operation == "Index") && perf.Started < nowTruncToSeconds
						let k = new { index, perf}
						group k by k.perf.Started.Ticks / TimeSpan.TicksPerSecond into g
                        orderby g.Key 
						select new
						{
							Started = new DateTime(g.Key * TimeSpan.TicksPerSecond, DateTimeKind.Utc),
							Stats = from k in g
                                    group k by k.index.Name into gg
									select new
									{
										Index = gg.Key,
                                        DurationMilliseconds = gg.Sum(x => x.perf.DurationMilliseconds),
                                        InputCount = gg.Sum(x => x.perf.InputCount),
                                        OutputCount = gg.Sum(x => x.perf.OutputCount),
                                        ItemsCount = gg.Sum(x => x.perf.ItemsCount)
									}
						};

			switch(format)
			{
				case "csv":
				case "CSV":
					var sw = new StringWriter();
					sw.WriteLine();
					foreach(var stat in stats)
					{
						sw.WriteLine(stat.Started.ToString("o"));
						sw.WriteLine("Index, Duration (ms), Input, Output, Items");
						foreach(var indexStat in stat.Stats)
						{
							sw.Write('"');
							sw.Write(indexStat.Index);
                            sw.Write("\",{0},{1},{2},{3}", indexStat.DurationMilliseconds, indexStat.InputCount, indexStat.OutputCount, indexStat.ItemsCount);
							sw.WriteLine();
						}
						sw.WriteLine();
					}
					var msg = sw.GetStringBuilder().ToString();
					return new HttpResponseMessage(HttpStatusCode.OK)
					{
						Content = new StringContent(msg)
						{
							Headers =
							{
								ContentType = new MediaTypeHeaderValue("text/plain")
							}
						}
					};
				default:
					return GetMessageWithObject(stats);
			}
		}


		[HttpGet]
		[Route("debug/plugins")]
		[Route("databases/{databaseName}/debug/plugins")]
		public HttpResponseMessage Plugins()
		{
			return GetMessageWithObject(Database.PluginsInfo);
		}

		[HttpGet]
		[Route("debug/changes")]
		[Route("databases/{databaseName}/debug/changes")]
		public HttpResponseMessage Changes()
		{
			return GetMessageWithObject(Database.TransportState.DebugStatuses);
		}

		[HttpGet]
		[Route("debug/sql-replication-stats")]
		[Route("databases/{databaseName}/debug/sql-replication-stats")]
		public HttpResponseMessage SqlReplicationStats()
		{
			var task = Database.StartupTasks.OfType<SqlReplicationTask>().FirstOrDefault();
			if (task == null)
				return GetMessageWithObject(new
				{
					Error = "SQL Replication bundle is not installed"
				}, HttpStatusCode.NotFound);


			//var metrics = task.SqlReplicationMetricsCounters.ToDictionary(x => x.Key, x => x.Value.ToSqlReplicationMetricsData());

			var statisticsAndMetrics = task.GetConfiguredReplicationDestinations().Select(x =>
			{
				SqlReplicationStatistics stats;
				task.Statistics.TryGetValue(x.Name, out stats);
				var metrics = task.GetSqlReplicationMetricsManager(x).ToSqlReplicationMetricsData();
				return new
				{
					x.Name,
					Statistics = stats,
					Metrics = metrics
				};
			});
			return GetMessageWithObject(statisticsAndMetrics);
		}


		[HttpGet]
		[Route("debug/metrics")]
		[Route("databases/{databaseName}/debug/metrics")]
		public HttpResponseMessage Metrics()
		{
			return GetMessageWithObject(Database.CreateMetrics());
		}

		[HttpGet]
		[Route("debug/config")]
		[Route("databases/{databaseName}/debug/config")]
		public HttpResponseMessage Config()
		{
			return GetMessageWithObject(DebugInfoProvider.GetConfigForDebug(Database));
		}

		[HttpGet]
		[Route("debug/docrefs")]
		[Route("databases/{databaseName}/debug/docrefs")]
		public HttpResponseMessage Docrefs(string id)
		{
			var op = GetQueryStringValue("op") == "from" ? "from" : "to";

			var totalCountReferencing = -1;
			List<string> results = null;
			Database.TransactionalStorage.Batch(accessor =>
			{
				totalCountReferencing = accessor.Indexing.GetCountOfDocumentsReferencing(id);
				var documentsReferencing =
					op == "from"
					? accessor.Indexing.GetDocumentsReferencesFrom(id)
					: accessor.Indexing.GetDocumentsReferencing(id);
				results = documentsReferencing.Skip(GetStart()).Take(GetPageSize(Database.Configuration.MaxPageSize)).ToList();
			});

			return GetMessageWithObject(new
			{
				TotalCountReferencing = totalCountReferencing,
				Results = results
			});
		}

		[HttpPost]
		[Route("debug/index-fields")]
		[Route("databases/{databaseName}/debug/index-fields")]
		public async Task<HttpResponseMessage> IndexFields()
		{
			var indexStr = await ReadStringAsync();
			bool querySyntax = indexStr.Trim().StartsWith("from");
			var mapDefinition = querySyntax
				? QueryParsingUtils.GetVariableDeclarationForLinqQuery(indexStr, true)
				: QueryParsingUtils.GetVariableDeclarationForLinqMethods(indexStr, true);

			var captureSelectNewFieldNamesVisitor = new CaptureSelectNewFieldNamesVisitor(querySyntax == false, new HashSet<string>(), new Dictionary<string, Expression>());
			mapDefinition.AcceptVisitor(captureSelectNewFieldNamesVisitor, null);

			return GetMessageWithObject(new { FieldNames = captureSelectNewFieldNamesVisitor.FieldNames });
		}

		[HttpGet]
		[Route("debug/list")]
		[Route("databases/{databaseName}/debug/list")]
		public HttpResponseMessage List(string id)
		{
			var listName = id;
			var key = InnerRequest.RequestUri.ParseQueryString()["key"];
			if (key == null)
				throw new ArgumentException("Key query string variable is mandatory");

			ListItem listItem = null;
			Database.TransactionalStorage.Batch(accessor =>
			{
				listItem = accessor.Lists.Read(listName, key);
			});

			if (listItem == null)
				return GetEmptyMessage(HttpStatusCode.NotFound);

			return GetMessageWithObject(listItem);
		}

		[HttpGet]
		[Route("debug/list-all")]
		[Route("databases/{databaseName}/debug/list-all")]
		public HttpResponseMessage ListAll(string id)
		{
			var listName = id;

			List<ListItem> listItems = null;
			Database.TransactionalStorage.Batch(accessor =>
			{
				listItems = accessor.Lists.Read(listName, Etag.Empty, null, GetPageSize(Database.Configuration.MaxPageSize)).ToList();
			});

			if (listItems == null)
				return GetEmptyMessage(HttpStatusCode.NotFound);

			return GetMessageWithObject(listItems);
		}

		[HttpGet]
		[Route("debug/queries")]
		[Route("databases/{databaseName}/debug/queries")]
		public HttpResponseMessage Queries()
		{
			return GetMessageWithObject(Database.WorkContext.CurrentlyRunningQueries);
		}

		[HttpGet]
		[Route("debug/suggest-index-merge")]
		[Route("databases/{databaseName}/debug/suggest-index-merge")]
		public HttpResponseMessage IndexMerge()
		{
			var mergeIndexSuggestions = Database.WorkContext.IndexDefinitionStorage.ProposeIndexMergeSuggestions();
			return GetMessageWithObject(mergeIndexSuggestions);
		}

		[HttpGet]
		[Route("debug/sl0w-d0c-c0unts")]
		[Route("databases/{databaseName}/debug/sl0w-d0c-c0unts")]
		public HttpResponseMessage SlowDocCounts()
		{
			DebugDocumentStats stat = null;
			Database.TransactionalStorage.Batch(accessor =>
			{
				stat = accessor.Documents.GetDocumentStatsVerySlowly();
			});

			return GetMessageWithObject(stat);
		}

		[HttpGet]
		[HttpPost]
		[Route("debug/user-info")]
		[Route("databases/{databaseName}/debug/user-info")]
		public HttpResponseMessage UserInfo()
		{
			var principal = User;
			if (principal == null || principal.Identity == null || principal.Identity.IsAuthenticated == false)
			{
				var anonymous = new UserInfo
				{
					Remark = "Using anonymous user",
					IsAdminGlobal = DatabasesLandlord.SystemConfiguration.AnonymousUserAccessMode == AnonymousUserAccessMode.Admin
				};
				return GetMessageWithObject(anonymous);
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

				return GetMessageWithObject(windowsUser);
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

				return GetMessageWithObject(windowsUserWithDatabase);
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

				return GetMessageWithObject(oAuth);
			}

			var unknown = new UserInfo
			{
				Remark = "Unknown auth",
				Principal = principal
			};

			return GetMessageWithObject(unknown);
		}

		[HttpGet]
		[Route("debug/tasks")]
		[Route("databases/{databaseName}/debug/tasks")]
		public HttpResponseMessage Tasks()
		{
			return GetMessageWithObject(DebugInfoProvider.GetTasksForDebug(Database));
		}

		[HttpGet]
		[Route("debug/routes")]
		[Description(@"Output the debug information for all the supported routes in Raven Server.")]
		public HttpResponseMessage Routes()
		{
			var routes = new SortedDictionary<string, RouteInfo>();

			foreach (var route in ControllerContext.Configuration.Routes)
			{
				var inner = route as IEnumerable<IHttpRoute>;
				if (inner == null) continue;

				foreach (var httpRoute in inner)
				{
					var key = httpRoute.RouteTemplate;
					bool forDatabase = false;
					if (key.StartsWith("databases/{databaseName}/"))
					{
						key = key.Substring("databases/{databaseName}/".Length);
						forDatabase = true;
					}
					var data = new RouteInfo(key);
					if (routes.ContainsKey(key))
						data = routes[key];

					if (forDatabase)
						data.CanRunForSpecificDatabase = true;

					var actions = ((IEnumerable)httpRoute.DataTokens["actions"]).OfType<ReflectedHttpActionDescriptor>();

					foreach (var reflectedHttpActionDescriptor in actions)
					{

						foreach (var httpMethod in reflectedHttpActionDescriptor.SupportedHttpMethods)
						{
							if (data.Methods.Any(method => method.Name == httpMethod.Method))
								continue;

							string description = null;
							var descriptionAttibute =
								reflectedHttpActionDescriptor.MethodInfo.CustomAttributes.FirstOrDefault(attributeData => attributeData.AttributeType == typeof(DescriptionAttribute));
							if (descriptionAttibute != null)
								description = descriptionAttibute.ConstructorArguments[0].Value.ToString();

							data.Methods.Add(new Method
							{
								Name = httpMethod.Method,
								Description = description
							});
						}
					}

					routes[key] = data;
				}
			}

			return GetMessageWithObject(routes);
		}

		[HttpGet]
		[Route("debug/currently-indexing")]
		[Route("databases/{databaseName}/debug/currently-indexing")]
		public HttpResponseMessage CurrentlyIndexing()
		{
			return GetMessageWithObject(DebugInfoProvider.GetCurrentlyIndexingForDebug(Database));
		}

		[HttpGet]
		[Route("debug/request-tracing")]
		[Route("databases/{databaseName}/debug/request-tracing")]
		public HttpResponseMessage RequestTracing()
		{
			return GetMessageWithObject(DebugInfoProvider.GetRequestTrackingForDebug(RequestManager, DatabaseName));
		}

		[HttpGet]
		[Route("debug/identities")]
		[Route("databases/{databaseName}/debug/identities")]
		public HttpResponseMessage Identities()
		{
			var start = GetStart();
			var pageSize = GetPageSize(1024);

			long totalCount = 0;
			IEnumerable<KeyValuePair<string, long>> identities = null;
			Database.TransactionalStorage.Batch(accessor => identities = accessor.General.GetIdentities(start, pageSize, out totalCount));

			return GetMessageWithObject(new
										{
											TotalCount = totalCount,
											Identities = identities
										});
		}

		[HttpGet]
		[Route("databases/{databaseName}/debug/info-package")]
		[Route("debug/info-package")]
		public HttpResponseMessage InfoPackage()
		{
			var tempFileName = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
			try
			{
				using (var file = new FileStream(tempFileName, FileMode.Create))
				using (var package = new ZipArchive(file, ZipArchiveMode.Create))
				{
					DebugInfoProvider.CreateInfoPackageForDatabase(package, Database, RequestManager);
				}

				var response = new HttpResponseMessage();

				response.Content = new StreamContent(new FileStream(tempFileName, FileMode.Open, FileAccess.Read))
								   {
									   Headers =
									   {
										   ContentDisposition = new ContentDispositionHeaderValue("attachment")
																{
																	FileName = string.Format("Debug-Info-{0}.zip", SystemTime.UtcNow),
																},
										   ContentType = new MediaTypeHeaderValue("application/octet-stream")
									   }
								   };

				return response;
			}
			finally
			{
				IOExtensions.DeleteFile(tempFileName);
			}
		}

		[HttpGet]
		[Route("databases/{databaseName}/debug/transactions")]
		[Route("debug/transactions")]
		public HttpResponseMessage Transactions()
		{
			return GetMessageWithObject(new
			{
				PreparedTransactions = Database.TransactionalStorage.GetPreparedTransactions()
			});
		}
	}

	public class RouteInfo
	{
		public string Key { get; set; }
		public List<Method> Methods { get; set; }

		public bool CanRunForSpecificDatabase { get; set; }

		public RouteInfo(string key)
		{
			Key = key;
			Methods = new List<Method>();
		}
	}

	public class Method
	{
		public string Name { get; set; }
		public string Description { get; set; }
	}
}