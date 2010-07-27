using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Database.Data;
using Raven.Database.Exceptions;
using Raven.Database.Extensions;
using Raven.Database.Json;
using Raven.Database.Server.Abstractions;

namespace Raven.Database.Server.Responders
{
	public static class HttpExtensions
	{
		static readonly Regex findCharset = new Regex(@"charset=([\w-]+)", RegexOptions.Compiled|RegexOptions.IgnoreCase);

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

		public static JObject ReadJson(this IHttpContext context)
		{
			using (var streamReader = new StreamReader(context.Request.InputStream, GetRequestEncoding(context)))
			using (var jsonReader = new JsonTextReader(streamReader))
				return JObject.Load(jsonReader);
		}

		public static T ReadJsonObject<T>(this IHttpContext context)
		{
			using (var streamReader = new StreamReader(context.Request.InputStream, GetRequestEncoding(context)))
			using (var jsonReader = new JsonTextReader(streamReader))
				return (T)new JsonSerializer
				{
					Converters = {new JsonEnumConverter()}
				}.Deserialize(jsonReader, typeof(T));
		}

		public static JArray ReadJsonArray(this IHttpContext context)
		{
			using (var streamReader = new StreamReader(context.Request.InputStream, GetRequestEncoding(context)))
			using (var jsonReader = new JsonTextReader(streamReader))
				return JArray.Load(jsonReader);
		}

		public static string ReadString(this IHttpContext context)
		{
			using (var streamReader = new StreamReader(context.Request.InputStream, GetRequestEncoding(context)))
				return streamReader.ReadToEnd();
		}

		public static void WriteJson(this IHttpContext context, object obj)
		{
			context.Response.Headers["Content-Type"] = "application/json; charset=utf-8";
			var streamWriter = new StreamWriter(context.Response.OutputStream);
			new JsonSerializer
			{
				Converters = {new JsonToJsonConverter(), new JsonEnumConverter()},
			}.Serialize(new JsonTextWriter(streamWriter)
			{
				Formatting = Formatting.Indented
			}, obj);
			streamWriter.Flush();
		}

		public static void WriteJson(this IHttpContext context, JToken obj)
		{
			context.Response.Headers["Content-Type"] = "application/json; charset=utf-8";
			var streamWriter = new StreamWriter(context.Response.OutputStream);
			var jsonTextWriter = new JsonTextWriter(streamWriter)
			{
				Formatting = Formatting.Indented
			};
			obj.WriteTo(jsonTextWriter, new JsonEnumConverter());
			jsonTextWriter.Flush();
			streamWriter.Flush();
		}

		public static void WriteData(this IHttpContext context, JObject data, JObject headers, Guid etag)
		{
			WriteData(context, Encoding.UTF8.GetBytes(data.ToString(Formatting.Indented)), headers, etag);
		}

		public static void WriteData(this IHttpContext context, byte[] data, JObject headers, Guid etag)
		{
			foreach (var header in headers.Properties())
			{
				if (header.Name.StartsWith("@"))
					continue;
				context.Response.Headers[header.Name] = StringQuotesIfNeeded(header.Value.ToString(Formatting.None));
			}
            if (headers["@Http-Status-Code"] != null)
            {
                context.Response.StatusCode = headers.Value<int>("@Http-Status-Code");
                context.Response.StatusDescription = headers.Value<string>("@Http-Status-Description");
            }
			context.Response.Headers["ETag"] = etag.ToString();
			context.Response.ContentLength64 = data.Length;
			context.Response.OutputStream.Write(data, 0, data.Length);
			context.Response.OutputStream.Flush();
		}

		private static string StringQuotesIfNeeded(string str)
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
			context.Response.Headers["Location"] = context.Configuration.GetFullUrl(location);
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

		public static void SetStatusToNonAuthoritiveInformation(this IHttpContext context)
		{
			context.Response.StatusCode = 203;
			context.Response.StatusDescription = "Non-Authoritative Information";
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
			return start;
		}

		public static bool GetAllowStale(this IHttpContext context)
		{
			bool stale;
			bool.TryParse(context.Request.QueryString["allowStale"], out stale);
			return stale;
		}

		public static int GetPageSize(this IHttpContext context, int maxPageSize)
		{
			int pageSize;
			int.TryParse(context.Request.QueryString["pageSize"], out pageSize);
			if (pageSize == 0)
				pageSize = 25;
			if (pageSize > maxPageSize)
                pageSize = maxPageSize;
			return pageSize;
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

		public static Guid? GetEtag(this IHttpContext context)
		{
			var etagAsString = context.Request.Headers["If-Match"];
			if (etagAsString != null)
			{
			    Guid result;
			    if (Guid.TryParse(etagAsString, out result))
			        return result;
			    throw new BadRequestException("Could not parse If-Match header as Guid");
			}
		    return null;
		}

		public static IndexQuery GetIndexQueryFromHttpContext(this IHttpContext context, int maxPageSize)
		{
			return new IndexQuery
			{
				Query = Uri.UnescapeDataString(context.Request.QueryString["query"] ?? ""),
				Start = context.GetStart(),
				Cutoff = context.GetCutOff(),
				PageSize = context.GetPageSize(maxPageSize),
				FieldsToFetch = context.Request.QueryString.GetValues("fetch"),
				SortedFields = context.Request.QueryString.GetValues("sort")
					.EmptyIfNull()
					.Select(x => new SortedField(x))
					.ToArray()
			};
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
			return context.Request.Headers["If-None-Match"] == etag.ToString();
		}

		public static void WriteEmbeddedFile(this IHttpContext context, string ravenPath, string docPath)
		{
			var filePath = Path.Combine(ravenPath, docPath);
			byte[] bytes;
			var etagValue = context.Request.Headers["If-Match"];
			context.Response.ContentType = GetContentType(docPath);
			if (File.Exists(filePath) == false)
			{
				string resourceName = "Raven.Database.Server.WebUI." + docPath.Replace("/", ".");
				if (etagValue == resourceName)
				{
					context.SetStatusToNotModified();
					return;
				}
				using (var resource = typeof(HttpExtensions).Assembly.GetManifestResourceStream(resourceName))
				{
					if (resource == null)
					{
						context.SetStatusToNotFound();
						return;
					}
					bytes = resource.ReadData();
				}
				context.Response.Headers["ETag"] = resourceName;
			}
			else
			{
				var fileEtag = File.GetLastWriteTimeUtc(filePath).ToString("G");
				if (etagValue == fileEtag)
				{
					context.SetStatusToNotModified();
					return;
				}
				bytes = File.ReadAllBytes(filePath);
				context.Response.Headers["ETag"] = fileEtag;
			}
			context.Response.OutputStream.Write(bytes, 0, bytes.Length);
			context.Response.OutputStream.Flush();
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
				default:
					return "text/plain";
			}
		}

		#region Nested type: JsonToJsonConverter

		public class JsonToJsonConverter : JsonConverter
		{
			public override void WriteJson (JsonWriter writer, object value, JsonSerializer serializer)
			{
				((JObject)value).WriteTo(writer);
			}

            public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
            {
				throw new NotImplementedException();
			}

			public override bool CanConvert (Type objectType)
			{
				return objectType == typeof(JObject);
			}
		}

		#endregion
	}
}