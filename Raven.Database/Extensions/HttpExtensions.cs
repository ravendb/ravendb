//-----------------------------------------------------------------------
// <copyright file="HttpExtensions.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Globalization;
using System.IO;
using System.Reflection;
using System.Security.Principal;
using System.Text;
using System.Text.RegularExpressions;
using Raven.Abstractions.Json;
using Raven.Imports.Newtonsoft.Json;
using Raven.Imports.Newtonsoft.Json.Bson;
using Raven.Imports.Newtonsoft.Json.Linq;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Database.Exceptions;
using Raven.Database.Server.Abstractions;
using Raven.Json.Linq;

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

		public static RavenJArray ReadJsonArray(this IHttpContext context)
		{
			using (var streamReader = new StreamReader(context.Request.InputStream, GetRequestEncoding(context)))
			using (var jsonReader = new RavenJsonTextReader(streamReader))
				return RavenJArray.Load(jsonReader);
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

		public static void WriteData(this IHttpContext context, RavenJObject data, RavenJObject headers, Guid etag)
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

		public static void WriteData(this IHttpContext context, byte[] data, RavenJObject headers, Guid etag)
		{
			context.WriteHeaders(headers, etag);
			context.Response.OutputStream.Write(data, 0, data.Length);
		}

		public static void WriteHeaders(this IHttpContext context, RavenJObject headers, Guid etag)
		{
			foreach (var header in headers)
			{
				if (header.Key.StartsWith("@"))
					continue;

				var value = GetHeaderValue(header.Value);
				switch (header.Key)
				{
					case "Content-Type":
						context.Response.ContentType = value;
						break;
					default:
						context.Response.AddHeader(header.Key, value);
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

		private static string GetHeaderValue(RavenJToken header)
		{
			if (header.Type == JTokenType.Date)
			{
				return header.Value<DateTime>().ToString("r");
			}

			return StripQuotesIfNeeded(header.ToString(Formatting.None));
		}

		private static string StripQuotesIfNeeded(string str)
		{
			if (str.StartsWith("\"") && str.EndsWith("\""))
				return str.Substring(1, str.Length - 2);
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

		public static void SetStatusToUnauthorized(this IHttpContext context)
		{
			context.Response.StatusCode = 401;
			context.Response.StatusDescription = "Unauthorized";
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
			if (self.Url.LocalPath.StartsWith(token, StringComparison.InvariantCultureIgnoreCase))
			{
				self.Url = new UriBuilder(self.Url)
				{
					Path = self.Url.LocalPath.Substring(token.Length)
				}.Uri;
			}
			if (self.RawUrl.StartsWith(token, StringComparison.InvariantCultureIgnoreCase))
			{
				self.RawUrl = self.RawUrl.Substring(token.Length);
				if(string.IsNullOrEmpty(self.RawUrl))
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

		public static Guid? GetCutOffEtag(this IHttpContext context)
		{
			var etagAsString = context.Request.QueryString["cutOffEtag"];
			if (etagAsString != null)
			{
				etagAsString = Uri.UnescapeDataString(etagAsString);

				Guid result;
				if (Guid.TryParse(etagAsString, out result))
					return result;
				throw new BadRequestException("Could not parse cut off etag query parameter as guid");
			}
			return null;
		}


		public static Guid? GetEtag(this IHttpContext context)
		{
			var etagAsString = context.Request.Headers["If-None-Match"] ?? context.Request.Headers["If-Match"];
			if (etagAsString != null)
			{
				Guid result;
				if (Guid.TryParse(etagAsString, out result))
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

		public static Guid? GetEtagFromQueryString(this IHttpContext context)
		{
			var etagAsString = context.Request.QueryString["etag"];
			if (etagAsString != null)
			{
				Guid result;
				if (Guid.TryParse(etagAsString, out result))
					return result;
				throw new BadRequestException("Could not parse etag query parameter as Guid");
			}
			return null;
		}

		public static bool MatchEtag(this IHttpContext context, Guid etag)
		{
			return EtagHeaderToGuid(context) == etag;
		}

		internal static Guid EtagHeaderToGuid(IHttpContext context)
		{
			var responseHeader = context.Request.Headers["If-None-Match"];
			if (string.IsNullOrEmpty(responseHeader))
				return Guid.NewGuid();

			if (responseHeader[0] == '\"')
				return new Guid(responseHeader.Substring(1, responseHeader.Length - 2));

			return new Guid(responseHeader);
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
			var fileEtag = File.GetLastWriteTimeUtc(filePath).ToString("G");
			if (etagValue == fileEtag)
			{
				context.SetStatusToNotModified();
				return;
			}
			context.WriteETag(fileEtag);
			context.Response.WriteFile(filePath);
		}


		public static void WriteETag(this IHttpContext context, Guid etag)
		{
			context.WriteETag(etag.ToString());
		}
		public static void WriteETag(this IHttpContext context, string etag)
		{
			var clientVersion = context.Request.Headers["Raven-Client-Version"];
			if (string.IsNullOrEmpty(clientVersion))
			{
				context.Response.AddHeader("ETag", etag);
				return;
			}

			context.Response.AddHeader("ETag", "\"" + etag + "\"");
		}

		public static bool IsAdministrator(this IPrincipal principal)
		{
			if (principal == null)
				return false;


			var windowsPrincipal = principal as WindowsPrincipal;
			if (windowsPrincipal != null)
			{
				// if the request was made using the same user as RavenDB is running as, we consider this
				// to be an administrator request

				var current = WindowsIdentity.GetCurrent();
				if (current != null && current.User == ((WindowsIdentity)windowsPrincipal.Identity).User)
					return true;

				return windowsPrincipal.IsInRole(WindowsBuiltInRole.Administrator);
			}

			return principal.IsInRole("Administrators");
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
