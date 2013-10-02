using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Runtime.InteropServices;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using System.Web;
using System.Web.Http;
using System.Web.Http.Controllers;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Indexing;
using Raven.Abstractions.Json;
using Raven.Abstractions.Util;
using Raven.Database.Extensions;
using Raven.Database.Server.Abstractions;
using Raven.Database.Server.Tenancy;
using System.Linq;
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
			get
			{
				var values = Request.GetRouteData().Values;
				if (values.ContainsKey("databaseName"))
					return Request.GetRouteData().Values["databaseName"] as string;
				return null;
			}
		}

		public static readonly Regex ChangesQuery = new Regex("^(/databases/([^/]+))?/changes/events", RegexOptions.IgnoreCase);

		public override Task<HttpResponseMessage> ExecuteAsync(HttpControllerContext controllerContext, CancellationToken cancellationToken)
		{
			var landlord = (DatabasesLandlord) controllerContext.Configuration.Properties[typeof (DatabasesLandlord)];
			landlord.IncrementRequestCount();
			var values = controllerContext.Request.GetRouteData().Values;
			string name;
			if (values.ContainsKey("databaseName"))
				name = controllerContext.Request.GetRouteData().Values["databaseName"] as string;
			else
				name = null;

			var msg = "Could not find a database named: " + name;

			if (name != null && landlord.GetDatabaseInternal(name) == null)
			{
				return new CompletedTask<HttpResponseMessage>(GetMessageWithObject(new
				                                                                   {
					                                                                   Error = msg
				                                                                   }, HttpStatusCode.ServiceUnavailable));
			}
			if (ChangesQuery.IsMatch(controllerContext.Request.RequestUri.AbsolutePath))
			{
				throw new NotImplementedException();
			}

			return base.ExecuteAsync(controllerContext, cancellationToken);
		}

		public DatabasesLandlord DatabasesLandlord
		{
			get
			{
				return (DatabasesLandlord)Configuration.Properties[typeof(DatabasesLandlord)];
			}
		}

		public DocumentDatabase Database
		{
			get
			{
				var database = DatabasesLandlord.GetDatabaseInternal(DatabaseName);
				if (database == null)
				{
					return null;
					throw new InvalidOperationException("Could not find a database named: " + DatabaseName);
				}

				return database.Result;
			}
		}

		public async Task<T> ReadJsonObjectAsync<T>()
		{
			using (var stream = await Request.Content.ReadAsStreamAsync())
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
			using (var stream = await Request.Content.ReadAsStreamAsync())
			using (var streamReader = new StreamReader(stream, GetRequestEncoding()))
			using (var jsonReader = new RavenJsonTextReader(streamReader))
				return RavenJObject.Load(jsonReader);
		}

		public async Task<RavenJArray> ReadJsonArrayAsync()
		{
			using (var stream = await Request.Content.ReadAsStreamAsync())
			using (var streamReader = new StreamReader(stream, GetRequestEncoding()))
			using (var jsonReader = new RavenJsonTextReader(streamReader))
				return RavenJArray.Load(jsonReader);
		}

		public async Task<string> ReadStringAsync()
		{
			using (var stream = await Request.Content.ReadAsStreamAsync())
			using (var streamReader = new StreamReader(stream, GetRequestEncoding()))
				return streamReader.ReadToEnd();
		}

		public async Task<RavenJArray> ReadBsonArrayAsync()
		{
			using (var stream = await Request.Content.ReadAsStreamAsync())
			using (var jsonReader = new BsonReader(stream))
			{
				var jObject = RavenJObject.Load(jsonReader);
				return new RavenJArray(jObject.Values<RavenJToken>());
			}
		}

		private Encoding GetRequestEncoding()
		{
			if (Request.Content.Headers.ContentType == null || string.IsNullOrWhiteSpace(Request.Content.Headers.ContentType.CharSet))
				return Encoding.GetEncoding("ISO-8859-1");
			return Encoding.GetEncoding(Request.Content.Headers.ContentType.CharSet);
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
			return Request.GetQueryNameValuePairs().Where(pair => pair.Key == key).Select(pair => pair.Value).FirstOrDefault();
		}

		public string[] GetQueryStringValues(string key)
		{
			var items = Request.GetQueryNameValuePairs().Where(pair => pair.Key == key);
			return items.Select(pair => pair.Value).ToArray();
		}

		public Etag GetEtagFromQueryString()
		{
			var etagAsString = GetQueryStringValue("etag");
			if (etagAsString != null)
			{
				return Etag.Parse(etagAsString);
			}
			return null;
		}

		protected TransactionInformation GetRequestTransaction()
		{
			if (Request.Headers.Contains("Raven-Transaction-Information") == false)
				return null;
			var txInfo = Request.Headers.GetValues("Raven-Transaction-Information").FirstOrDefault();
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
				GroupBy = GetQueryStringValues("groupBy"),
				DefaultField = GetQueryStringValue("defaultField"),

				DefaultOperator =
					string.Equals(GetQueryStringValue("operator"), "AND", StringComparison.OrdinalIgnoreCase) ?
						QueryOperator.And :
						QueryOperator.Or,

				AggregationOperation = GetAggregationOperation(),
				SortedFields = EnumerableExtension.EmptyIfNull(GetQueryStringValues("sort"))
					.Select(x => new SortedField(x))
					.ToArray(),
				HighlightedFields = GetHighlightedFields().ToArray(),
				HighlighterPreTags = GetQueryStringValues("preTags"),
				HighlighterPostTags = GetQueryStringValues("postTags"),
				ResultsTransformer = GetQueryStringValue("resultsTransformer"),
				QueryInputs = ExtractQueryInputs()
			};


			var spatialFieldName = GetQueryStringValue("spatialField") ?? Constants.DefaultSpatialFieldName;
			var queryShape = GetQueryStringValue("queryShape");
			SpatialUnits units;
			bool unitsSpecified = Enum.TryParse(GetQueryStringValue("spatialUnits"), out units);
			double distanceErrorPct;
			if (!double.TryParse(GetQueryStringValue("distErrPrc"), out distanceErrorPct))
				distanceErrorPct = Constants.DefaultSpatialDistanceErrorPct;
			SpatialRelation spatialRelation;
			if (Enum.TryParse(GetQueryStringValue("spatialRelation"), false, out spatialRelation)
				&& !string.IsNullOrWhiteSpace(queryShape))
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

		public AggregationOperation GetAggregationOperation()
		{
			var aggAsString = GetQueryStringValue("aggregation");
			if (aggAsString == null)
			{
				return AggregationOperation.None;
			}

			return (AggregationOperation)Enum.Parse(typeof(AggregationOperation), aggAsString, true);
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
					throw new BadRequestException(
						"Could not parse highlight query parameter as field highlight options");
			}
		}

		public Dictionary<string, RavenJToken> ExtractQueryInputs()
		{
			var result = new Dictionary<string, RavenJToken>();
			foreach (var key in Request.GetQueryNameValuePairs().Select(pair => pair.Key))
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
						msg.Content.Headers.ContentType = new MediaTypeHeaderValue(header.Value.Value<string>());
						break;
					default:
						if (header.Value.Type == JTokenType.Date)
						{
							var rfc1123 = GetDateString(header.Value, "r");
							var iso8601 = GetDateString(header.Value, "o");
							msg.Content.Headers.Add(header.Key, rfc1123);
							if (header.Key.StartsWith("Raven-") == false)
							{
								msg.Content.Headers.Add("Raven-" + header.Key, iso8601);
							}
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

			return msg;
		}

		public HttpResponseMessage GetMessageWithString(string msg, HttpStatusCode code = HttpStatusCode.OK, Etag etag = null)
		{
			var resMsg = new HttpResponseMessage(code)
			{
				Content = new JsonContent(msg)
			};
			WriteETag(etag, resMsg);

			return resMsg;
		}

		private static readonly Encoding DefaultEncoding = new UTF8Encoding(false);
		public HttpResponseMessage WriteData(RavenJObject data, RavenJObject headers, Etag etag, HttpStatusCode status = HttpStatusCode.OK, HttpResponseMessage msg = null)
		{
			if (msg == null)
				msg = new HttpResponseMessage(status);

			var jsonContent = ((JsonContent) msg.Content);
			var jsonp = GetQueryStringValue("jsonp");

			WriteHeaders(headers, etag, msg);

			if (string.IsNullOrEmpty(jsonp) == false)
			{
				msg.Content.Headers.ContentType = new MediaTypeHeaderValue("application/javascript") {CharSet = "utf-8"};
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
			if (Request.Headers.Contains(key) == false)
				return null;
			return Request.Headers.GetValues(key).FirstOrDefault();
		}

		public List<string> GetHeaders(string key)
		{
			if (Request.Headers.Contains(key) == false)
				return null;
			return Request.Headers.GetValues(key).ToList();
		}

		public bool HasCookie(string key)
		{
			return Request.Headers.GetCookies(key).Count != 0;
		}

		public string GetCookie(string key)
		{
			var cookieHeaderValue = Request.Headers.GetCookies(key).FirstOrDefault();
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
				return WriteFile(filePath, type);
			return WriteEmbeddedFileOfType(docPath, type);
		}

		public HttpResponseMessage WriteFile(string filePath, string type)
		{
			var etagValue = GetHeader("If-None-Match") ?? GetHeader("If-None-Match");
			var fileEtag = File.GetLastWriteTimeUtc(filePath).ToString("G");
			if (etagValue == fileEtag)
			{
				return new HttpResponseMessage(HttpStatusCode.NotModified);
			}

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
			{
				return new HttpResponseMessage(HttpStatusCode.NotModified);
			}

			byte[] bytes;
			string resourceName = "Raven.Database.Server.WebUI." + docPath.Replace("/", ".");
			//TODO: check the typeof
			using (var resource = typeof(IHttpContext).Assembly.GetManifestResourceStream(resourceName))
			{
				if (resource == null)
				{
					return new HttpResponseMessage(HttpStatusCode.NotFound);
				}
				bytes = resource.ReadData();
			}
			var msg = new HttpResponseMessage
			{
				Content = new ByteArrayContent(bytes),
			};

			msg.Headers.Add("Content-Type", type);
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
			var rawUrl = Request.RequestUri.AbsoluteUri;
			return UrlExtension.GetRequestUrlFromRawUrl(rawUrl, DatabasesLandlord.SystemConfiguration);
		}
	}
}