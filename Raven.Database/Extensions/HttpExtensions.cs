//-----------------------------------------------------------------------
// <copyright file="HttpExtensions.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.IO.Compression;
using System.Reflection;
using System.Security.Principal;
using System.Text;
using System.Text.RegularExpressions;
using System.Web;
using Raven.Abstractions.Json;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Util;
using Raven.Imports.Newtonsoft.Json;
using Raven.Imports.Newtonsoft.Json.Bson;
using Raven.Imports.Newtonsoft.Json.Linq;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Exceptions;
using Raven.Database.Server.Abstractions;
using Raven.Json.Linq;
using System.Linq;
using Raven.Abstractions.Connection;

namespace Raven.Database.Extensions
{
	public static class HttpExtensions
	{
		static readonly Regex findCharset = new Regex(@"charset=([\w-]+)", RegexOptions.Compiled | RegexOptions.IgnoreCase);

		private static readonly string EmbeddedLastChangedDate =
			File.GetLastWriteTime(typeof(HttpExtensions).Assembly.Location).Ticks.ToString("G");

		private static readonly Encoding defaultEncoding = new UTF8Encoding(false);


		private static Encoding GetRequestEncoding(IHttpContext context)
		{
			var contentType = context.Request.Headers["Content-Type"];
			if (contentType == null)
				return Encoding.GetEncoding("ISO-8859-1");
			var match = findCharset.Match(contentType);
			if (match.Success == false)
				return Encoding.GetEncoding("ISO-8859-1");
			return Encoding.GetEncoding(match.Groups[1].Value);
		}

		public static RavenJObject ReadJson(this IHttpContext context)
		{
			using (var streamReader = new StreamReader(context.Request.InputStream, GetRequestEncoding(context)))
			using (var jsonReader = new RavenJsonTextReader(streamReader))
				return RavenJObject.Load(jsonReader);
		}

		public static T ReadJsonObject<T>(this IHttpContext context)
		{
			using (var streamReader = new StreamReader(context.Request.InputStream, GetRequestEncoding(context)))
			{
				var readToEnd = streamReader.ReadToEnd();
				using (var jsonReader = new RavenJsonTextReader(new StringReader(readToEnd)))
				{
					var result = JsonExtensions.CreateDefaultJsonSerializer();

					return (T)result.Deserialize(jsonReader, typeof(T));
				}
			}
		}

		public static RavenJArray ReadJsonArray(this IHttpContext context, out long bytesRead)
		{
            using (var stream = context.Request.GetBufferLessInputStream())
            using (var countingStream = new CountingStream(stream))
            using (var actualStream = GetRequestStream(countingStream, context))
            using (var streamReader = new StreamReader(actualStream, GetRequestEncoding(context)))
            using (var jsonReader = new RavenJsonTextReader(streamReader))
            {
                var loadedJson = RavenJArray.Load(jsonReader);
                bytesRead = countingStream.Position;
                return loadedJson;
            }
		}

	    private static Stream GetRequestStream(Stream stream, IHttpContext context)
	    {
	        var contentEncoding = context.Request.Headers["Content-Encoding"];
	        if (contentEncoding == null ||
	            contentEncoding.Equals("gzip", StringComparison.OrdinalIgnoreCase) == false)
	        {
	            return stream;
	        }

	        return new GZipStream(stream, CompressionMode.Decompress);
	    }

		public static RavenJArray ReadBsonArray(this IHttpContext context)
		{
			using (var jsonReader = new BsonReader(context.Request.InputStream))
			{
				var jObject = RavenJObject.Load(jsonReader);
				return new RavenJArray(jObject.Values<RavenJToken>());
			}
		}

		public static string ReadString(this IHttpContext context)
		{
			using (var streamReader = new StreamReader(context.Request.InputStream, GetRequestEncoding(context)))
				return streamReader.ReadToEnd();
		}

		public static void WriteJson(this IHttpContext context, object obj)
		{
			WriteJson(context, RavenJToken.FromObject(obj));
		}

		public static void WriteJson(this IHttpContext context, RavenJToken obj)
		{
			if (context.Request.HttpMethod == "HEAD")
				return;

			bool minimal;
			bool.TryParse(context.Request.QueryString["metadata-only"], out minimal);

			var streamWriter = new StreamWriter(context.Response.OutputStream, defaultEncoding);
			var jsonp = context.Request.QueryString["jsonp"];
			if (string.IsNullOrEmpty(jsonp) == false)
			{
				context.Response.AddHeader("Content-Type", "application/javascript; charset=utf-8");
				streamWriter.Write(jsonp);
				streamWriter.Write("(");
			}
			else
			{
				context.Response.AddHeader("Content-Type", "application/json; charset=utf-8");

			}

			if (minimal)
			{
				obj = MinimizeToken(obj);
			}

			var jsonTextWriter = new JsonTextWriter(streamWriter)
			{
				Formatting = Formatting.None
			};
			obj.WriteTo(jsonTextWriter, Default.Converters);
			jsonTextWriter.Flush();
			if (string.IsNullOrEmpty(jsonp) == false)
			{
				streamWriter.Write(");");
			}
			streamWriter.Flush();
		}

		public static RavenJToken MinimizeToken(RavenJToken obj, int depth = 0)
		{
			switch (obj.Type)
			{
				case JTokenType.Array:
					var array = new RavenJArray();
					foreach (var item in ((RavenJArray)obj))
					{
						array.Add(MinimizeToken(item, depth + 1));
					}
					return array;
				case JTokenType.Object:
					var ravenJObject = ((RavenJObject)obj);
					if (ravenJObject.ContainsKey(Constants.Metadata) == false)
					{
						// this might be a wrapper object, let check for first level arrays
						if (depth == 0)
						{
							var newRootObj = new RavenJObject();

							foreach (var prop in ravenJObject)
							{
								newRootObj[prop.Key] = prop.Value.Type == JTokenType.Array ?
									MinimizeToken(prop.Value, depth + 1) :
									prop.Value;
							}
							return newRootObj;
						}
						return obj;
					}
					var newObj = new RavenJObject();
					newObj[Constants.Metadata] = ravenJObject[Constants.Metadata];
					return newObj;
				default:
					return obj;
			}
		}

		public static void WriteData(this IHttpContext context, RavenJObject data, RavenJObject headers, Etag etag)
		{
			var str = data.ToString(Formatting.None);
			var jsonp = context.Request.QueryString["jsonp"];
			if (string.IsNullOrEmpty(jsonp) == false)
			{
				str = jsonp + "(" + str + ");";
				context.Response.AddHeader("Content-Type", "application/javascript; charset=utf-8");
			}
			else
			{
				context.Response.AddHeader("Content-Type", "application/json; charset=utf-8");
			}
			WriteData(context, defaultEncoding.GetBytes(str), headers, etag);
		}

		public static void WriteData(this IHttpContext context, byte[] data, RavenJObject headers, Etag etag)
		{
			context.WriteHeaders(headers, etag);
			context.Response.OutputStream.Write(data, 0, data.Length);
		}

		public static void WriteHeaders(this IHttpContext context, RavenJObject headers, Etag etag)
		{
			foreach (var header in headers)
			{
				if (header.Key.StartsWith("@"))
					continue;

				switch (header.Key)
				{
					case "Content-Type":
						context.Response.ContentType = header.Value.Value<string>();
						break;
					default:
						if (header.Value.Type == JTokenType.Date)
						{
							var rfc1123 = GetDateString(header.Value, "r");
							var iso8601 = GetDateString(header.Value, "o");
							context.Response.AddHeader(header.Key, rfc1123);
							if (header.Key.StartsWith("Raven-") == false)
							{
								context.Response.AddHeader("Raven-" + header.Key, iso8601);
							}
						}
						else
						{
							var value = UnescapeStringIfNeeded(header.Value.ToString(Formatting.None));
							context.Response.AddHeader(header.Key, value);
						}
						break;
				}
			}
			if (headers["@Http-Status-Code"] != null)
			{
				context.Response.StatusCode = headers.Value<int>("@Http-Status-Code");
				context.Response.StatusDescription = headers.Value<string>("@Http-Status-Description");
			}
			context.WriteETag(etag);
		}

		private static string GetDateString(RavenJToken token, string format)
		{
			var value = token as RavenJValue;
			if (value == null)
				return token.ToString();

			var obj = value.Value;

			if (obj is DateTime)
				return ((DateTime)obj).ToString(format, CultureInfo.InvariantCulture);

			if (obj is DateTimeOffset)
				return ((DateTimeOffset)obj).ToString(format, CultureInfo.InvariantCulture);

			return obj.ToString();
		}

		private static string UnescapeStringIfNeeded(string str)
		{
			if (str.StartsWith("\"") && str.EndsWith("\""))
				str =  Regex.Unescape(str.Substring(1, str.Length - 2));
			if (str.Any(ch => ch > 127))
			{
				// contains non ASCII chars, needs encoding
				return Uri.EscapeDataString(str);
			}
			return str;
		}

		public static void SetStatusToDeleted(this IHttpContext context)
		{
			context.Response.StatusCode = 204;
			context.Response.StatusDescription = "No Content";
		}

		public static void SetStatusToCreated(this IHttpContext context, string location)
		{
			context.Response.StatusCode = 201;
			context.Response.StatusDescription = "Created";
			context.Response.AddHeader("Location", context.Configuration.GetFullUrl(location));
		}


		public static void SetStatusToWriteConflict(this IHttpContext context)
		{
			context.Response.StatusCode = 409;
			context.Response.StatusDescription = "Conflict";
		}

		public static void SetStatusToNotFound(this IHttpContext context)
		{
			context.Response.StatusCode = 404;
			context.Response.StatusDescription = "Not Found";
		}

		public static void SetStatusToNotModified(this IHttpContext context)
		{
			context.Response.StatusCode = 304;
			context.Response.StatusDescription = "Not Modified";
		}

		public static void SetStatusToNonAuthoritativeInformation(this IHttpContext context)
		{
			context.Response.StatusCode = 203;
			context.Response.StatusDescription = "Non-Authoritative Information";
		}

		public static void SetStatusToBadRequest(this IHttpContext context)
		{
			context.Response.StatusCode = 400;
			context.Response.StatusDescription = "Bad Request";
		}

		public static void SetStatusToPreconditionFailed(this IHttpContext context)
		{
			context.Response.StatusCode = 412;
			context.Response.StatusDescription = "Precondition Failed";
		}

		public static void SetStatusToUnauthorized(this IHttpContext context)
		{
			context.Response.StatusCode = 401;
			context.Response.StatusDescription = "Unauthorized";
		}


		public static void SetSerializationException(this IHttpContext context, Exception e)
		{
			context.SetStatusToUnprocessableEntity();
			const string errorMessage = "Could not understand json that was sent.";
			context.WriteJson(new
			{
				Message = errorMessage
			});

			context.Log(log => log.Error(errorMessage + " Exception that was thrown: " + e));
		}

		public static void SetStatusToUnprocessableEntity(this IHttpContext context)
		{
			context.Response.StatusCode = 422;
			context.Response.StatusDescription = "Unprocessable Entity";
		}

		public static void SetStatusToNotAvailable(this IHttpContext context)
		{
			context.Response.StatusCode = 503;
			context.Response.StatusDescription = "Service Unavailable";
		}

		public static void SetStatusToForbidden(this IHttpContext context)
		{
			context.Response.StatusCode = 403;
			context.Response.StatusDescription = "Forbidden";
		}

		public static void Write(this IHttpContext context, string str)
		{
			var sw = new StreamWriter(context.Response.OutputStream);
			sw.Write(str);
			sw.Flush();
		}

		public static int GetStart(this IHttpContext context)
		{
			int start;
			int.TryParse(context.Request.QueryString["start"], out start);
			return Math.Max(0, start);
		}

		public static bool GetAllowStale(this IHttpContext context)
		{
			bool stale;
			bool.TryParse(context.Request.QueryString["allowStale"], out stale);
			return stale;
		}

		public static void AdjustUrl(this IHttpContext self, string token)
		{
			self.Request.RemoveFromRequestUrl(token);
			self.Response.RedirectionPrefix = token;

		}

		public static void RemoveFromRequestUrl(this IHttpRequest self, string token)
		{
			if (self.Url.LocalPath.StartsWith(token, StringComparison.OrdinalIgnoreCase))
			{
				self.Url = new UriBuilder(self.Url)
				{
					Path = self.Url.LocalPath.Substring(token.Length)
				}.Uri;
			}
			if (self.RawUrl.StartsWith(token, StringComparison.OrdinalIgnoreCase))
			{
				self.RawUrl = self.RawUrl.Substring(token.Length);
				if (string.IsNullOrEmpty(self.RawUrl))
				{
					self.RawUrl = "/";
				}
			}
		}

		public static bool GetSkipTransformResults(this IHttpContext context)
		{
			bool result;
			bool.TryParse(context.Request.QueryString["skipTransformResults"], out result);
			return result;
		}

		public static bool GetCheckForUpdates(this IHttpContext context)
		{
			bool result;
			bool.TryParse(context.Request.QueryString["checkForUpdates"], out result);
			return result;
		}

		public static bool GetCheckReferencesInIndexes(this IHttpContext context)
		{
			bool result;
			bool.TryParse(context.Request.QueryString["checkReferencesInIndexes"], out result);
			return result;
		}

		public static int GetPageSize(this IHttpContext context, int maxPageSize)
		{
			int pageSize;
			if (int.TryParse(context.Request.QueryString["pageSize"], out pageSize) == false || pageSize < 0)
				pageSize = 25;
			if (pageSize > maxPageSize)
				pageSize = maxPageSize;
			return pageSize;
		}

		public static AggregationOperation GetAggregationOperation(this IHttpContext context)
		{
			var aggAsString = context.Request.QueryString["aggregation"];
			if (aggAsString == null)
			{
				return AggregationOperation.None;
			}

			return (AggregationOperation)Enum.Parse(typeof(AggregationOperation), aggAsString, true);
		}

		public static DateTime? GetCutOff(this IHttpContext context)
		{
			var etagAsString = context.Request.QueryString["cutOff"];
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

		public static Etag GetCutOffEtag(this IHttpContext context)
		{
			var etagAsString = context.Request.QueryString["cutOffEtag"];
			if (etagAsString != null)
			{
				etagAsString = Uri.UnescapeDataString(etagAsString);

				return Etag.Parse(etagAsString);
			}
			return null;
		}


		public static Etag GetEtag(this IHttpContext context)
		{
			var etagAsString = context.Request.Headers["If-None-Match"] ?? context.Request.Headers["If-Match"];
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

		public static double GetLat(this IHttpContext context)
		{
			double lat;
			double.TryParse(context.Request.QueryString["latitude"], NumberStyles.Any, CultureInfo.InvariantCulture, out lat);
			return lat;
		}

		public static double GetLng(this IHttpContext context)
		{
			double lng;
			double.TryParse(context.Request.QueryString["longitude"], NumberStyles.Any, CultureInfo.InvariantCulture, out lng);
			return lng;
		}

		public static double GetRadius(this IHttpContext context)
		{
			double radius;
			double.TryParse(context.Request.QueryString["radius"], NumberStyles.Any, CultureInfo.InvariantCulture, out radius);
			return radius;
		}

		public static IEnumerable<HighlightedField> GetHighlightedFields(this IHttpContext context)
		{
			var highlightedFieldStrings = context.Request.QueryString.GetValues("highlight").EmptyIfNull();
			var fields = new HashSet<string>();

			foreach (var highlightedFieldString in highlightedFieldStrings)
			{
				HighlightedField highlightedField;
				if (HighlightedField.TryParse(highlightedFieldString, out highlightedField))
				{
					if (!fields.Add(highlightedField.Field))
						throw new BadRequestException("Duplicate highlighted field has found: " + highlightedField.Field);

					yield return highlightedField;
				} else
					throw new BadRequestException(
						"Could not parse hightlight query parameter as field highlight options");
			}
		}

		public static Etag GetEtagFromQueryString(this IHttpContext context)
		{
			var etagAsString = context.Request.QueryString["etag"];
			if (etagAsString != null)
			{
				return Etag.Parse(etagAsString);
			}
			return null;
		}

		public static bool MatchEtag(this IHttpContext context, Etag etag)
		{
			return EtagHeaderToEtag(context) == etag;
		}

		internal static Etag EtagHeaderToEtag(IHttpContext context)
		{
			var responseHeader = context.Request.Headers["If-None-Match"];
			if (string.IsNullOrEmpty(responseHeader))
				return Etag.InvalidEtag;

			if (responseHeader[0] == '\"')
				return Etag.Parse(responseHeader.Substring(1, responseHeader.Length - 2));

			return Etag.Parse(responseHeader);
		}

		public static void WriteEmbeddedFile(this IHttpContext context, string ravenPath, string docPath)
		{
			var filePath = Path.Combine(ravenPath, docPath);
			context.Response.ContentType = GetContentType(docPath);
			if (File.Exists(filePath))
				WriteFile(context, filePath);
			else
				WriteEmbeddedFile(context, docPath);
		}

		private static void WriteEmbeddedFile(this IHttpContext context, string docPath)
		{
			var etagValue = context.Request.Headers["If-None-Match"] ?? context.Request.Headers["If-Match"];
			var currentFileEtag = EmbeddedLastChangedDate + docPath;
			if (etagValue == currentFileEtag)
			{
				context.SetStatusToNotModified();
				return;
			}

			byte[] bytes;
			string resourceName = "Raven.Database.Server.WebUI." + docPath.Replace("/", ".");
			using (var resource = typeof(IHttpContext).Assembly.GetManifestResourceStream(resourceName))
			{
				if (resource == null)
				{
					context.SetStatusToNotFound();
					return;
				}
				bytes = resource.ReadData();
			}
			context.WriteETag(currentFileEtag);
			context.Response.OutputStream.Write(bytes, 0, bytes.Length);
		}

		public static void WriteFile(this IHttpContext context, string filePath)
		{
			var etagValue = context.Request.Headers["If-None-Match"] ?? context.Request.Headers["If-None-Match"];
			var fileEtag = File.GetLastWriteTimeUtc(filePath).ToString("G", CultureInfo.InvariantCulture);
			if (etagValue == fileEtag)
			{
				context.SetStatusToNotModified();
				return;
			}
			context.WriteETag(fileEtag);
			context.Response.WriteFile(filePath);
		}


		public static void WriteETag(this IHttpContext context, Etag etag)
		{
			context.WriteETag(etag.ToString());
		}
		public static void WriteETag(this IHttpContext context, string etag)
		{
			var clientVersion = context.Request.Headers[Constants.RavenClientVersion];
			if (string.IsNullOrEmpty(clientVersion))
			{
				context.Response.AddHeader("ETag", etag);
				return;
			}

			context.Response.AddHeader("ETag", "\"" + etag + "\"");
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
	}
}
