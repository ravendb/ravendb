using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
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
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Logging;
using Raven.Database.Bundles.SqlReplication;
using Raven.Database.Extensions;
using Raven.Database.Linq;
using Raven.Database.Linq.Ast;
using Raven.Database.Server.Abstractions;
using Raven.Database.Storage;
using Raven.Database.Tasks;
using Raven.Database.Util;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;
using Index = Raven.Database.Indexing.Index;
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
			return GetMessageWithObject(GetPrefetchingQueueStatusForDebug());
		}

		private object GetPrefetchingQueueStatusForDebug()
		{
			var prefetcherDocs = Database.IndexingExecuter.PrefetchingBehavior.DebugGetDocumentsInPrefetchingQueue().ToArray();
			var compareToCollection = new Dictionary<Etag, int>();

			for (int i = 1; i < prefetcherDocs.Length; i++)
				compareToCollection.Add(prefetcherDocs[i - 1].Etag, prefetcherDocs[i].Etag.CompareTo(prefetcherDocs[i - 1].Etag));

			if (compareToCollection.Any(x => x.Value < 0))
			{
				return new
				{
					HasCorrectlyOrderedEtags = true,
					EtagsWithKeys = prefetcherDocs.ToDictionary(x => x.Etag, x => x.Key)
				};
			}

			return new
			{
				HasCorrectlyOrderedEtags = false,
				IncorrectlyOrderedEtags = compareToCollection.Where(x => x.Value < 0),
				EtagsWithKeys = prefetcherDocs.ToDictionary(x => x.Etag, x => x.Key)
			};
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

			return GetMessageWithObject(task.Statistics);
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
			return GetMessageWithObject(GetConfigForDebug());
		}

		private RavenJObject GetConfigForDebug()
		{
			var cfg = RavenJObject.FromObject(Database.Configuration);
			cfg["OAuthTokenKey"] = "<not shown>";
			var changesAllowed = Database.Configuration.Settings["Raven/Versioning/ChangesToRevisionsAllowed"];

			if (string.IsNullOrWhiteSpace(changesAllowed) == false)
				cfg["Raven/Versioning/ChangesToRevisionsAllowed"] = changesAllowed;

			return cfg;
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
			var mapDefinition = indexStr.Trim().StartsWith("from")
				? QueryParsingUtils.GetVariableDeclarationForLinqQuery(indexStr, true)
				: QueryParsingUtils.GetVariableDeclarationForLinqMethods(indexStr, true);

			var captureSelectNewFieldNamesVisitor = new CaptureSelectNewFieldNamesVisitor();
			mapDefinition.AcceptVisitor(captureSelectNewFieldNamesVisitor, null);

			return GetMessageWithObject(new { captureSelectNewFieldNamesVisitor.FieldNames });
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
					IsAdminGlobal =
						windowsPrincipal.IsAdministrator(DatabasesLandlord.SystemConfiguration.AnonymousUserAccessMode)
				};

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
			return GetMessageWithObject(GetTasksForDebug());
		}

		private IList<TaskMetadata> GetTasksForDebug()
		{
			IList<TaskMetadata> tasks = null;
			Database.TransactionalStorage.Batch(accessor =>
			{
				tasks = accessor.Tasks
					.GetPendingTasksForDebug()
					.ToList();
			});

			foreach (var taskMetadata in tasks)
			{
				var indexInstance = Database.IndexStorage.GetIndexInstance(taskMetadata.IndexId);
				if (indexInstance != null)
					taskMetadata.IndexName = indexInstance.PublicName;
			}
			return tasks;
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
							if(descriptionAttibute != null)
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
			return GetMessageWithObject(GetCurrentlyIndexingForDebug());
		}

		private object GetCurrentlyIndexingForDebug()
		{
			var indexingWork = Database.IndexingExecuter.GetCurrentlyProcessingIndexes();
			var reduceWork = Database.ReducingExecuter.GetCurrentlyProcessingIndexes();

			var uniqueIndexesBeingProcessed = indexingWork.Union(reduceWork).Distinct(new Index.IndexByIdEqualityComparer()).ToList();

			return new
			{
				NumberOfCurrentlyWorkingIndexes = uniqueIndexesBeingProcessed.Count,
				Indexes = uniqueIndexesBeingProcessed.Select(x => new
				{
					IndexName = x.PublicName,
					IsMapReduce = x.IsMapReduce,
					CurrentOperations = x.GetCurrentIndexingPerformance().Select(p => new { p.Operation, NumberOfProcessingItems = p.InputCount}),
					Priority = x.Priority,
					OverallIndexingRate = x.GetIndexingPerformance().Where(ip => ip.Duration != TimeSpan.Zero).GroupBy(y => y.Operation).Select(g => new
					{
						Operation = g.Key,
						Rate = string.Format("{0:0.0000} ms/doc", g.Sum(z => z.Duration.TotalMilliseconds) / g.Sum(z => z.InputCount))
					})
				})
			};
		}

		[HttpGet]
		[Route("debug/request-tracing")]
		[Route("databases/{databaseName}/debug/request-tracing")]
		public HttpResponseMessage RequestTracing()
		{
			return GetMessageWithObject(GetRequestTrackingForDebug());
		}

		private object GetRequestTrackingForDebug()
		{
			return RequestManager.GetRecentRequests(DatabaseName).Select(x => new
			{
				Uri = x.RequestUri,
				Method = x.HttpMethod,
				StatusCode = x.ResponseStatusCode,
				RequestHeaders = x.Headers.AllKeys.Select(k => new { Name = k, Values = x.Headers.GetValues(k)}),
				ExecutionTime = string.Format("{0} ms", x.Stopwatch.ElapsedMilliseconds),
				AdditionalInfo = x.CustomInfo ?? string.Empty
			});
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
		[Route("debug/info-package")]
		[Route("databases/{databaseName}/debug/info-package")]
		public HttpResponseMessage InfoPackage()
		{
			var compressionLevel = CompressionLevel.Optimal;

			var tempFileName = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());

			using (var file = new FileStream(tempFileName, FileMode.Create))
			using (var package = new ZipArchive(file, ZipArchiveMode.Create))
			{
				var jsonSerializer = new JsonSerializer
				{
					Formatting = Formatting.Indented
				};

				var stats = package.CreateEntry("stats.txt", compressionLevel);

				using (var statsStream = stats.Open())
				using (var streamWriter = new StreamWriter(statsStream))
				{
					jsonSerializer.Serialize(streamWriter, Database.Statistics);
					streamWriter.Flush();
				}

				var metrics = package.CreateEntry("metrics.txt", compressionLevel);

				using (var metricsStream = metrics.Open())
				using (var streamWriter = new StreamWriter(metricsStream))
				{
					jsonSerializer.Serialize(streamWriter, Database.CreateMetrics());
					streamWriter.Flush();
				}

				var logs = package.CreateEntry("logs.txt", compressionLevel);

				using (var logsStream = logs.Open())
				using (var streamWriter = new StreamWriter(logsStream))
				{
					var target = LogManager.GetTarget<DatabaseMemoryTarget>();

					if (target == null)
						streamWriter.WriteLine("DatabaseMemoryTarget was not registered in the log manager, logs are not available");
					else
					{
						var dbName = DatabaseName ?? Constants.SystemDatabase;
						var boundedMemoryTarget = target[dbName];
						var log = boundedMemoryTarget.GeneralLog;

						streamWriter.WriteLine("time,logger,level,message,exception");

						foreach (var logEvent in log)
						{
							streamWriter.WriteLine("{0:O},{1},{2},{3},{4}", logEvent.TimeStamp, logEvent.LoggerName, logEvent.Level, logEvent.FormattedMessage, logEvent.Exception);
						}
					}

					streamWriter.Flush();
				}

				var config = package.CreateEntry("config.txt", compressionLevel);

				using (var configStream = config.Open())
				using (var streamWriter = new StreamWriter(configStream))
				{
					jsonSerializer.Serialize(streamWriter, GetConfigForDebug());
					streamWriter.Flush();
				}

				var currentlyIndexing = package.CreateEntry("currently-indexing.txt", compressionLevel);

				using (var currentlyIndexingStream = currentlyIndexing.Open())
				using (var streamWriter = new StreamWriter(currentlyIndexingStream))
				{
					jsonSerializer.Serialize(streamWriter, GetCurrentlyIndexingForDebug());
					streamWriter.Flush();
				}

				var queries = package.CreateEntry("queries.txt", compressionLevel);

				using (var queriesStream = queries.Open())
				using (var streamWriter = new StreamWriter(queriesStream))
				{
					jsonSerializer.Serialize(streamWriter, Database.WorkContext.CurrentlyRunningQueries);
					streamWriter.Flush();
				}

				var prefetchStatus = package.CreateEntry("prefetch-status.txt", compressionLevel);

				using (var prefetchStatusStream = prefetchStatus.Open())
				using (var streamWriter = new StreamWriter(prefetchStatusStream))
				{
					jsonSerializer.Serialize(streamWriter, GetPrefetchingQueueStatusForDebug());
					streamWriter.Flush();
				}

				var requestTracking = package.CreateEntry("request-tracking.txt", compressionLevel);

				using (var requestTrackingStream = requestTracking.Open())
				using (var streamWriter = new StreamWriter(requestTrackingStream))
				{
					jsonSerializer.Serialize(streamWriter, GetRequestTrackingForDebug());
					streamWriter.Flush();
				}

				var tasks = package.CreateEntry("tasks.txt", compressionLevel);

				using (var tasksStream = tasks.Open())
				using (var streamWriter = new StreamWriter(tasksStream))
				{
					jsonSerializer.Serialize(streamWriter, GetTasksForDebug());
					streamWriter.Flush();
				}

				var stacktraceRequsted = GetQueryStringValue("stacktrace");

				if (stacktraceRequsted != null)
				{
					var stacktrace = package.CreateEntry("stacktraces.txt", compressionLevel);

					using (var stacktraceStream = stacktrace.Open())
					{
						string ravenDebugDir = null;

						try
						{
							if (Debugger.IsAttached)
								throw new InvalidOperationException("Cannot get stacktraces when debugger is attached");

							ravenDebugDir = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
							var ravenDebugExe = Path.Combine(ravenDebugDir, "Raven.Debug.exe");
							var ravenDebugOutput = Path.Combine(ravenDebugDir, "stacktraces.txt");

							Directory.CreateDirectory(ravenDebugDir);

							if (Environment.Is64BitProcess)
								ExtractResource("Raven.Database.Util.Raven.Debug.x64.Raven.Debug.exe", ravenDebugExe);
							else
								ExtractResource("Raven.Database.Util.Raven.Debug.x86.Raven.Debug.exe", ravenDebugExe);

							var process = new Process
							{
								StartInfo = new ProcessStartInfo
								{
									Arguments = string.Format("-pid={0} /stacktrace -output={1}", Process.GetCurrentProcess().Id, ravenDebugOutput),
									FileName = ravenDebugExe,
									WindowStyle = ProcessWindowStyle.Hidden,
								}
							};

							process.Start();

							process.WaitForExit();

							using (var stackDumpOutputStream = File.Open(ravenDebugOutput, FileMode.Open))
							{
								stackDumpOutputStream.CopyTo(stacktraceStream);
							}
						}
						catch (Exception ex)
						{
							var streamWriter = new StreamWriter(stacktraceStream);
							streamWriter.WriteLine("Exception occurred during getting stacktraces of the RavenDB process. Exception: " + ex);
						}
						finally
						{
							if (ravenDebugDir != null && Directory.Exists(ravenDebugDir))
								IOExtensions.DeleteDirectory(ravenDebugDir);
						}

						stacktraceStream.Flush();
					}
				}

				file.Flush();
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

		private void ExtractResource(string resource, string path)
		{
			var stream = GetType().Assembly.GetManifestResourceStream(resource);

			if(stream == null)
				throw new InvalidOperationException("Could not find the requested resource: " + resource);

			var bytes = new byte[4096];

			using (var stackDump = File.Create(path, 4096))
			{
				while (true)
				{
					var read = stream.Read(bytes, 0, bytes.Length);
					if(read == 0)
						break;

					stackDump.Write(bytes, 0, read);
				}

				stackDump.Flush();
			}
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