using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Security.Principal;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using System.Web;
using System.Web.Http;
using System.Web.Http.Controllers;
using System.Web.Http.Routing;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Indexing;
using Raven.Abstractions.Json;
using Raven.Bundles.Replication.Tasks;
using Raven.Database.Extensions;
using Raven.Database.Server.Abstractions;
using Raven.Database.Server.Security;
using Raven.Database.Server.Tenancy;
using System.Linq;
using Raven.Database.Server.WebApi;
using Raven.Imports.Newtonsoft.Json;
using Raven.Imports.Newtonsoft.Json.Bson;
using Raven.Imports.Newtonsoft.Json.Linq;
using Raven.Json.Linq;

namespace Raven.Database.Server.Controllers
{
	public abstract class RavenApiController : ApiController
	{
		public string DatabaseName
		{
			get;
			private set;
		}

		public HttpRequestMessage InnerRequest
		{
			get
			{
				return Request ?? request;
			}
		}

		public HttpHeaders InnerHeaders
		{
			get
			{
				var headers = new Headers();
				foreach (var header in InnerRequest.Headers)
				{
					if (header.Value.Count() == 1)
						headers.Add(header.Key, header.Value.First());
					else
						headers.Add(header.Key, header.Value.ToList());
				}

				if (InnerRequest.Content == null) 
					return headers;
				
				foreach (var header in InnerRequest.Content.Headers)
				{
					if (header.Value.Count() == 1)
						headers.Add(header.Key, header.Value.First());
					else
						headers.Add(header.Key, header.Value.ToList());
				}

				return headers;
			}
		}

		public new IPrincipal User
		{
			get;
			set;
		}

		private HttpRequestMessage request;
		public override async Task<HttpResponseMessage> ExecuteAsync(HttpControllerContext controllerContext, CancellationToken cancellationToken)
		{
			InnerInitialization(controllerContext);
			var authorizer = (MixedModeRequestAuthorizer)controllerContext.Configuration.Properties[typeof(MixedModeRequestAuthorizer)];
			var result = new HttpResponseMessage();

			try
			{
				await RequestManager.HandleActualRequest(this, async () =>
				{
					SetHeaders();
					result = await ExecuteActualRequest(controllerContext, cancellationToken, authorizer);
				});
			}
			catch (HttpException httpException)
			{
				result = GetMessageWithObject(new { Error = httpException.Message }, HttpStatusCode.ServiceUnavailable);
			}

			RequestManager.AddAccessControlHeaders(this, result);
			return result;
		}

		private void SetHeaders()
		{
			foreach (var innerHeader in InnerHeaders)
			{
				CurrentOperationContext.Headers.Value[innerHeader.Key] = innerHeader.Value.FirstOrDefault();
			}
		}

		private async Task<HttpResponseMessage> ExecuteActualRequest(HttpControllerContext controllerContext, CancellationToken cancellationToken,
			MixedModeRequestAuthorizer authorizer)
		{
			HttpResponseMessage authMsg;
			if (authorizer.TryAuthorize(this, out authMsg) == false)
				return authMsg;

			var internalHeader = GetHeader("Raven-internal-request");
			if (internalHeader == null || internalHeader != "true")
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

		protected void InnerInitialization(HttpControllerContext controllerContext)
		{
			landlord = (DatabasesLandlord)controllerContext.Configuration.Properties[typeof(DatabasesLandlord)];
			requestManager = (RequestManager)controllerContext.Configuration.Properties[typeof(RequestManager)];
			request = controllerContext.Request;
			User = controllerContext.RequestContext.Principal;

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

		private void AddRavenHeader(HttpResponseMessage msg, Stopwatch sp)
		{
			AddHeader("Raven-Server-Build", DocumentDatabase.BuildVersion, msg);
			AddHeader("Temp-Request-Time", sp.ElapsedMilliseconds.ToString("#,#;;0", CultureInfo.InvariantCulture), msg);
		}

		private void AddAccessControlHeaders(HttpResponseMessage msg)
		{
			if (string.IsNullOrEmpty(DatabasesLandlord.SystemConfiguration.AccessControlAllowOrigin))
				return;

			AddHeader("Access-Control-Allow-Credentials", "true", msg);

			var originAllowed = DatabasesLandlord.SystemConfiguration.AccessControlAllowOrigin == "*" ||
					DatabasesLandlord.SystemConfiguration.AccessControlAllowOrigin.Split(' ')
						.Any(o => o == GetHeader("Origin"));
			if (originAllowed)
			{
				AddHeader("Access-Control-Allow-Origin", GetHeader("Origin"), msg);
			}

			AddHeader("Access-Control-Max-Age", DatabasesLandlord.SystemConfiguration.AccessControlMaxAge, msg);
			AddHeader("Access-Control-Allow-Methods", DatabasesLandlord.SystemConfiguration.AccessControlAllowMethods, msg);

			if (string.IsNullOrEmpty(DatabasesLandlord.SystemConfiguration.AccessControlRequestHeaders))
			{
				// allow whatever headers are being requested
				var hdr = GetHeader("Access-Control-Request-Headers"); // typically: "x-requested-with"
				if (hdr != null) AddHeader("Access-Control-Allow-Headers", hdr, msg);
			}
			else
			{
				AddHeader("Access-Control-Request-Headers", DatabasesLandlord.SystemConfiguration.AccessControlRequestHeaders, msg);
			}
		}

		private DatabasesLandlord landlord;
		public DatabasesLandlord DatabasesLandlord
		{
			get
			{
				if (Configuration == null)
					return landlord;
				return (DatabasesLandlord)Configuration.Properties[typeof(DatabasesLandlord)];
			}
		}

		private RequestManager requestManager;
		public RequestManager RequestManager
		{
			get
			{
				if (Configuration == null)
					return requestManager;
				return (RequestManager)Configuration.Properties[typeof(RequestManager)];
			}
		}

		public DocumentDatabase Database
		{
			get
			{
				var database = DatabasesLandlord.GetDatabaseInternal(DatabaseName);
				if (database == null)
				{
					throw new InvalidOperationException("Could not find a database named: " + DatabaseName);
				}

				return database.Result;
			}
		}

		public async Task<T> ReadJsonObjectAsync<T>()
		{
			using (var stream = await InnerRequest.Content.ReadAsStreamAsync())
			//using(var gzipStream = new GZipStream(stream, CompressionMode.Decompress))
			using (var streamReader = new StreamReader(stream, GetRequestEncoding()))
			{
				using (var jsonReader = new JsonTextReader(streamReader))
				{
					var result = JsonExtensions.CreateDefaultJsonSerializer();

					return (T)result.Deserialize(jsonReader, typeof(T));
				}
			}
		}

		public async Task<RavenJObject> ReadJsonAsync()
		{
			using (var stream = await InnerRequest.Content.ReadAsStreamAsync())
			using (var streamReader = new StreamReader(stream, GetRequestEncoding()))
			using (var jsonReader = new RavenJsonTextReader(streamReader))
				return RavenJObject.Load(jsonReader);
		}

		public async Task<RavenJArray> ReadJsonArrayAsync()
		{
			using (var stream = await InnerRequest.Content.ReadAsStreamAsync())
			using (var streamReader = new StreamReader(stream, GetRequestEncoding()))
			using (var jsonReader = new RavenJsonTextReader(streamReader))
				return RavenJArray.Load(jsonReader);
		}

		public async Task<string> ReadStringAsync()
		{
			using (var stream = await InnerRequest.Content.ReadAsStreamAsync())
			using (var streamReader = new StreamReader(stream, GetRequestEncoding()))
				return streamReader.ReadToEnd();
		}

		public async Task<RavenJArray> ReadBsonArrayAsync()
		{
			using (var stream = await InnerRequest.Content.ReadAsStreamAsync())
			using (var jsonReader = new BsonReader(stream))
			{
				var jObject = RavenJObject.Load(jsonReader);
				return new RavenJArray(jObject.Values<RavenJToken>());
			}
		}

		private Encoding GetRequestEncoding()
		{
			if (InnerRequest.Content.Headers.ContentType == null || string.IsNullOrWhiteSpace(InnerRequest.Content.Headers.ContentType.CharSet))
				return Encoding.GetEncoding("ISO-8859-1");
			return Encoding.GetEncoding(InnerRequest.Content.Headers.ContentType.CharSet);
		}

		protected bool EnsureSystemDatabase()
		{
			return DatabasesLandlord.SystemDatabase == Database;
		}

		public int GetStart()
		{
			int start;
			int.TryParse(GetQueryStringValue("start"), out start);
			return Math.Max(0, start);
		}

		public int GetPageSize(int maxPageSize)
		{
			int pageSize;
			if (int.TryParse(GetQueryStringValue("pageSize"), out pageSize) == false || pageSize < 0)
				pageSize = 25;
			if (pageSize > maxPageSize)
				pageSize = maxPageSize;
			return pageSize;
		}

		public bool MatchEtag(Etag etag)
		{
			return EtagHeaderToEtag() == etag;
		}

		internal Etag EtagHeaderToEtag()
		{
			var responseHeader = GetHeader("If-None-Match");
			if (string.IsNullOrEmpty(responseHeader))
				return Etag.InvalidEtag;

			if (responseHeader[0] == '\"')
				return Etag.Parse(responseHeader.Substring(1, responseHeader.Length - 2));

			return Etag.Parse(responseHeader);
		}

		public string GetQueryStringValue(string key)
		{
			return GetQueryStringValue(InnerRequest, key);
		}

		public static string GetQueryStringValue(HttpRequestMessage req, string key)
		{
			return req.GetQueryNameValuePairs().Where(pair => pair.Key == key).Select(pair => pair.Value).FirstOrDefault();
		}

		public string[] GetQueryStringValues(string key)
		{
			var items = InnerRequest.GetQueryNameValuePairs().Where(pair => pair.Key == key);
			return items.Select(pair => pair.Value).ToArray();
		}

		public Etag GetEtagFromQueryString()
		{
			var etagAsString = GetQueryStringValue("etag");
			return etagAsString != null ? Etag.Parse(etagAsString) : null;
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

		protected IndexQuery GetIndexQuery(int maxPageSize)
		{
			var query = new IndexQuery
			{
				Query = GetQueryStringValue("query") ?? "",
				Start = GetStart(),
				Cutoff = GetCutOff(),
				CutoffEtag = GetCutOffEtag(),
				PageSize = GetPageSize(maxPageSize),
				SkipTransformResults = GetSkipTransformResults(),
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
				ResultsTransformer = GetQueryStringValue("resultsTransformer"),
				QueryInputs = ExtractQueryInputs(),
				ExplainScores = GetExplainScores(),
				SortHints = GetSortHints(),
				IsDistinct = IsDistinct()
			};


			var spatialFieldName = GetQueryStringValue("spatialField") ?? Constants.DefaultSpatialFieldName;
			var queryShape = GetQueryStringValue("queryShape");
			SpatialUnits units;
			var unitsSpecified = Enum.TryParse(GetQueryStringValue("spatialUnits"), out units);
			double distanceErrorPct;
			if (!double.TryParse(GetQueryStringValue("distErrPrc"), out distanceErrorPct))
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

			foreach (var header in InnerRequest.Headers.Where(pair => pair.Key.StartsWith("SortHint-")))
			{
				SortOptions sort;
				Enum.TryParse(GetHeader(header.Key), true, out sort);
				result.Add(header.Key, sort);
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

		public bool GetSkipTransformResults()
		{
			bool result;
			bool.TryParse(GetQueryStringValue("skipTransformResults"), out result);
			return result;
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

		public Dictionary<string, RavenJToken> ExtractQueryInputs()
		{
			var result = new Dictionary<string, RavenJToken>();
			foreach (var key in InnerRequest.GetQueryNameValuePairs().Select(pair => pair.Key))
			{
				if (string.IsNullOrEmpty(key)) continue;
				if (key.StartsWith("qp-"))
				{
					var realkey = key.Substring(3);
					result[realkey] = GetQueryStringValue(key);
				}
			}

			return result;
		}

		public void WriteETag(Etag etag, HttpResponseMessage msg)
		{
			if (etag == null)
				return;
			WriteETag(etag.ToString(), msg);
		}

		public void WriteETag(string etag, HttpResponseMessage msg)
		{
			if (string.IsNullOrWhiteSpace(etag))
				return;
			//string clientVersion = GetHeader("Raven-Client-Version");
			//if (string.IsNullOrEmpty(clientVersion))
			//{
			//	msg.Headers.ETag = new EntityTagHeaderValue(etag);
			//	return;
			//}

			msg.Headers.ETag = new EntityTagHeaderValue("\"" + etag + "\"");
		}

		public void WriteHeaders(RavenJObject headers, Etag etag, HttpResponseMessage msg)
		{
			foreach (var header in headers)
			{
				if (header.Key.StartsWith("@"))
					continue;

				switch (header.Key)
				{
					case "Content-Type":
						var headerValue = header.Value.Value<string>();
						string charset = null;
						if (headerValue.Contains("charset="))
						{
							var splits = headerValue.Split(';');
							headerValue = splits[0];

							charset = splits[1].Split('=')[1];
						}

						msg.Content.Headers.ContentType = new MediaTypeHeaderValue(headerValue){CharSet = charset};
						
						break;
					default:
						if (header.Value.Type == JTokenType.Date)
						{
							var rfc1123 = GetDateString(header.Value, "r");
							var iso8601 = GetDateString(header.Value, "o");
							msg.Content.Headers.Add(header.Key, rfc1123);
							if (header.Key.StartsWith("Raven-") == false)
								msg.Content.Headers.Add("Raven-" + header.Key, iso8601);
						}
						else
						{
							var value = UnescapeStringIfNeeded(header.Value.ToString(Formatting.None));
							msg.Content.Headers.Add(header.Key, value);
						}
						break;
				}
			}
			if (headers["@Http-Status-Code"] != null)
			{
				msg.StatusCode = (HttpStatusCode)headers.Value<int>("@Http-Status-Code");
				msg.Content.Headers.Add("Temp-Status-Description", headers.Value<string>("@Http-Status-Description"));
			}

			WriteETag(etag, msg);
		}

		public void AddHeader(string key, string value, HttpResponseMessage msg)
		{
			if (msg.Content == null)
				msg.Content = new JsonContent();
			msg.Content.Headers.Add(key, value);
		}

		private string GetDateString(RavenJToken token, string format)
		{
			var value = token as RavenJValue;
			if (value == null)
				return token.ToString();

			var obj = value.Value;

			if (obj is DateTime)
				return ((DateTime)obj).ToString(format);

			if (obj is DateTimeOffset)
				return ((DateTimeOffset)obj).ToString(format);

			return obj.ToString();
		}

		private static string UnescapeStringIfNeeded(string str)
		{
			if (str.StartsWith("\"") && str.EndsWith("\""))
				str = Regex.Unescape(str.Substring(1, str.Length - 2));
			if (str.Any(ch => ch > 127))
			{
				// contains non ASCII chars, needs encoding
				return Uri.EscapeDataString(str);
			}
			return str;
		}

		public HttpResponseMessage GetMessageWithObject(object item, HttpStatusCode code = HttpStatusCode.OK, Etag etag = null)
		{
			var token = item as RavenJToken ?? RavenJToken.FromObject(item);
			var msg = new HttpResponseMessage(code)
			{
				Content = new JsonContent(token),
			};

			WriteETag(etag, msg);

			AddAccessControlHeaders(msg);
			HandleReplication(msg);

			return msg;
		}

		public HttpResponseMessage GetMessageWithString(string msg, HttpStatusCode code = HttpStatusCode.OK, Etag etag = null)
		{
			var resMsg = new HttpResponseMessage(code)
			{
				Content = new JsonContent(msg)
			};

			WriteETag(etag, resMsg);

			AddAccessControlHeaders(resMsg);
			HandleReplication(resMsg);

			return resMsg;
		}

		public HttpResponseMessage GetEmptyMessage(HttpStatusCode code = HttpStatusCode.OK, Etag etag = null)
		{
			var resMsg = new HttpResponseMessage(code)
			{
				Content = new JsonContent()
			};
			WriteETag(etag, resMsg);

			AddAccessControlHeaders(resMsg);
			HandleReplication(resMsg);

			return resMsg;
		}

		private static readonly Encoding DefaultEncoding = new UTF8Encoding(false);
		public HttpResponseMessage WriteData(RavenJObject data, RavenJObject headers, Etag etag, HttpStatusCode status = HttpStatusCode.OK, HttpResponseMessage msg = null)
		{
			if (msg == null)
				msg = GetEmptyMessage(status);

			var jsonContent = ((JsonContent)msg.Content);
			var jsonp = GetQueryStringValue("jsonp");

			WriteHeaders(headers, etag, msg);

			if (string.IsNullOrEmpty(jsonp) == false)
			{
				msg.Content.Headers.ContentType = new MediaTypeHeaderValue("application/javascript") { CharSet = "utf-8" };
				jsonContent.Jsonp = jsonp;
			}
			else
			{
				msg.Content.Headers.ContentType = new MediaTypeHeaderValue("application/json") { CharSet = "utf-8" };
			}

			jsonContent.Token = data;

			return msg;
		}

		public Etag GetEtag()
		{
			var etagAsString = GetHeader("If-None-Match") ?? GetHeader("If-Match");
			if (etagAsString != null)
			{
				// etags are usually quoted
				if (etagAsString.StartsWith("\"") && etagAsString.EndsWith("\""))
					etagAsString = etagAsString.Substring(1, etagAsString.Length - 2);

				Etag result;
				if (Etag.TryParse(etagAsString, out result))
					return result;

				throw new BadRequestException("Could not parse If-None-Match or If-Match header as Guid");
			}

			return null;
		}

		public string GetHeader(string key)
		{
			if (InnerHeaders.Contains(key) == false)
				return null;
			return InnerHeaders.GetValues(key).FirstOrDefault();
		}

		public List<string> GetHeaders(string key)
		{
			if (InnerHeaders.Contains(key) == false)
				return null;
			return InnerHeaders.GetValues(key).ToList();
		}

		public bool HasCookie(string key)
		{
			return InnerRequest.Headers.GetCookies(key).Count != 0;
		}

		public string GetCookie(string key)
		{
			var cookieHeaderValue = InnerRequest.Headers.GetCookies(key).FirstOrDefault();
			if (cookieHeaderValue != null)
			{
				var coockie = cookieHeaderValue.Cookies.FirstOrDefault();
				if (coockie != null)
					return coockie.Value;
			}

			return null;
		}

		protected bool GetCheckForUpdates()
		{
			bool result;
			bool.TryParse(GetQueryStringValue("checkForUpdates"), out result);
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

		//TODO: check
		private static readonly string EmbeddedLastChangedDate =
			File.GetLastWriteTime(typeof(HttpExtensions).Assembly.Location).Ticks.ToString("G");

		public HttpResponseMessage WriteEmbeddedFile(string ravenPath, string docPath)
		{
			var filePath = Path.Combine(ravenPath, docPath);
			var type = GetContentType(docPath);
			if (File.Exists(filePath))
				return WriteFile(filePath);
			return WriteEmbeddedFileOfType(docPath, type);
		}

		public HttpResponseMessage WriteFile(string filePath)
		{
			var etagValue = GetHeader("If-None-Match") ?? GetHeader("If-None-Match");
			var fileEtag = File.GetLastWriteTimeUtc(filePath).ToString("G");
			if (etagValue == fileEtag)
				return GetEmptyMessage(HttpStatusCode.NotModified);

			var msg = new HttpResponseMessage
			{
				Content = new StreamContent(new FileStream(filePath, FileMode.Open))
			};

			WriteETag(fileEtag, msg);

			return msg;
		}

		private HttpResponseMessage WriteEmbeddedFileOfType(string docPath, string type)
		{
			var etagValue = GetHeader("If-None-Match") ?? GetHeader("If-Match");
			var currentFileEtag = EmbeddedLastChangedDate + docPath;
			if (etagValue == currentFileEtag)
				return GetEmptyMessage(HttpStatusCode.NotModified);

			byte[] bytes;
			var resourceName = "Raven.Database.Server.WebUI." + docPath.Replace("/", ".");

			using (var resource = typeof(IHttpContext).Assembly.GetManifestResourceStream(resourceName))
			{
				if (resource == null)
					return GetEmptyMessage(HttpStatusCode.NotFound);

				bytes = resource.ReadData();
			}
			var msg = new HttpResponseMessage
			{
				Content = new ByteArrayContent(bytes),
			};

			msg.Content.Headers.ContentType = new MediaTypeHeaderValue(type);
			WriteETag(etagValue, msg);

			return msg;
		}

		private static string GetContentType(string docPath)
		{
			switch (Path.GetExtension(docPath))
			{
				case ".html":
				case ".htm":
					return "text/html";
				case ".css":
					return "text/css";
				case ".js":
					return "text/javascript";
				case ".ico":
					return "image/vnd.microsoft.icon";
				case ".jpg":
					return "image/jpeg";
				case ".gif":
					return "image/gif";
				case ".png":
					return "image/png";
				case ".xap":
					return "application/x-silverlight-2";
				default:
					return "text/plain";
			}
		}

		public string GetRequestUrl()
		{
			var rawUrl = InnerRequest.RequestUri.PathAndQuery;
			return UrlExtension.GetRequestUrlFromRawUrl(rawUrl, DatabasesLandlord.SystemConfiguration);
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
	}

	public class Headers : HttpHeaders
	{
		
	}
}