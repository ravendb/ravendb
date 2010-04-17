using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.IO;
using System.Linq;
using System.Net;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Raven.Server.Responders
{
	public static class HttpExtensions
	{
		public static JObject ReadJson(this HttpListenerContext context)
		{
			using (var streamReader = new StreamReader(context.Request.InputStream))
			using (var jsonReader = new JsonTextReader(streamReader))
				return JObject.Load(jsonReader);
		}

		public static T ReadJsonObject<T>(this HttpListenerContext context)
		{
			using (var streamReader = new StreamReader(context.Request.InputStream))
			using (var jsonReader = new JsonTextReader(streamReader))
				return (T)new JsonSerializer().Deserialize(jsonReader, typeof(T));
		}

		public static JArray ReadJsonArray(this HttpListenerContext context)
		{
			using (var streamReader = new StreamReader(context.Request.InputStream))
			using (var jsonReader = new JsonTextReader(streamReader))
				return JArray.Load(jsonReader);
		}

		public static string ReadString(this HttpListenerContext context)
		{
			using (var streamReader = new StreamReader(context.Request.InputStream))
				return streamReader.ReadToEnd();
		}

		public static void WriteJson(this HttpListenerContext context, object obj)
		{
			context.Response.Headers["Content-Type"] = "application/json; charset=utf-8";
			var streamWriter = new StreamWriter(context.Response.OutputStream);
			new JsonSerializer
			{
				Converters = {new JsonToJsonConverter()}
			}.Serialize(streamWriter, obj);
			streamWriter.Flush();
		}

		public static void WriteJson(this HttpListenerContext context, JToken obj)
		{
			context.Response.Headers["Content-Type"] = "application/json; charset=utf-8";
			var streamWriter = new StreamWriter(context.Response.OutputStream);
			var jsonTextWriter = new JsonTextWriter(streamWriter);
			obj.WriteTo(jsonTextWriter);
			jsonTextWriter.Flush();
			streamWriter.Flush();
		}

		public static void WriteData(this HttpListenerContext context, byte[] data, JObject headers, Guid etag)
		{
			foreach (var header in headers.Properties())
			{
				if (header.Name.StartsWith("@"))
					continue;
				context.Response.Headers[header.Name] = StringQuotesIfNeeded(header.Value.ToString());
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

		public static void SetStatusToDeleted(this HttpListenerContext context)
		{
			context.Response.StatusCode = 204;
			context.Response.StatusDescription = "No Content";
		}

		public static void SetStatusToCreated(this HttpListenerContext context, string location)
		{
			context.Response.StatusCode = 201;
			context.Response.StatusDescription = "Created";
			context.Response.Headers["Location"] = location;
		}


		public static void SetStatusToWriteConflict(this HttpListenerContext context)
		{
			context.Response.StatusCode = 409;
			context.Response.StatusDescription = "Conflict";
		}

		public static void SetStatusToNotFound(this HttpListenerContext context)
		{
			context.Response.StatusCode = 404;
			context.Response.StatusDescription = "Not Found";
		}

		public static void SetStatusToNotModified(this HttpListenerContext context)
		{
			context.Response.StatusCode = 304;
			context.Response.StatusDescription = "Not Modified";
		}

		public static void SetStatusToBadRequest(this HttpListenerContext context)
		{
			context.Response.StatusCode = 400;
			context.Response.StatusDescription = "Bad Request";
		}

		public static void SetStatusToUnauthorized(this HttpListenerContext context)
		{
			context.Response.StatusCode = 401;
			context.Response.StatusDescription = "Unauthorized";
		}

		public static void Write(this HttpListenerContext context, string str)
		{
			var sw = new StreamWriter(context.Response.OutputStream);
			sw.Write(str);
			sw.Flush();
		}

		/// <summary>
		/// 	Reads the entire request buffer to memory and
		/// 	return it as a byte array.
		/// </summary>
		public static byte[] ReadData(this Stream steram)
		{
			var list = new List<byte[]>();
			const int defaultBufferSize = 1024*16;
			var buffer = new byte[defaultBufferSize];
			var offset = 0;
			int read;
			while ((read = steram.Read(buffer, offset, buffer.Length - offset)) != 0)
			{
				offset += read;
				if (offset == buffer.Length)
				{
					list.Add(buffer);
					buffer = new byte[defaultBufferSize];
					offset = 0;
				}
			}
			var totalSize = list.Sum(x => x.Length) + offset;
			var result = new byte[totalSize];
			var resultOffset = 0;
			foreach (var partial in list)
			{
				Buffer.BlockCopy(partial, 0, result, resultOffset, partial.Length);
				resultOffset += partial.Length;
			}
			Buffer.BlockCopy(buffer, 0, result, resultOffset, offset);
			return result;
		}

		public static int GetStart(this HttpListenerContext context)
		{
			int start;
			int.TryParse(context.Request.QueryString["start"], out start);
			return start;
		}

		public static int GetPageSize(this HttpListenerContext context)
		{
			int pageSize;
			int.TryParse(context.Request.QueryString["pageSize"], out pageSize);
			if (pageSize == 0)
				pageSize = 25;
			if (pageSize > 1024)
				pageSize = 1024;
			return pageSize;
		}

		public static Guid? GetEtag(this HttpListenerContext context)
		{
			var etagAsString = context.Request.Headers["If-Match"];
			if (etagAsString != null)
			{
				try
				{
					return new Guid(etagAsString);
				}
				catch
				{
					throw new BadRequestException("Could not parse If-Match header as Guid");
				}
			}
			return null;
		}

		public static bool MatchEtag(this HttpListenerContext context, Guid etag)
		{
			return context.Request.Headers["If-None-Match"] == etag.ToString();
		}

		public static void WriteEmbeddedFile(this HttpListenerContext context, string ravenPath, string docPath)
		{
			var filePath = Path.Combine(ravenPath, docPath);
			byte[] bytes;
			if (File.Exists(filePath) == false)
			{
				string resourceName = "Raven.Server.WebUI." + docPath.Replace("/", ".");
				using (var resource = typeof(HttpExtensions).Assembly.GetManifestResourceStream(resourceName))
				{
					if (resource == null)
					{
						context.SetStatusToNotFound();
						return;
					}
					bytes = resource.ReadData();
				}
			}
			else
			{
				bytes = File.ReadAllBytes(filePath);
			}
			context.Response.OutputStream.Write(bytes, 0, bytes.Length);
			context.Response.OutputStream.Flush();
		}

		#region Nested type: JsonToJsonConverter

		public class JsonToJsonConverter : JsonConverter
		{
			public override
				void WriteJson
				(JsonWriter writer, object value)
			{
				((JObject) value).WriteTo(writer);
			}

			public override
				object ReadJson
				(JsonReader reader, Type objectType)
			{
				throw new NotImplementedException();
			}

			public override
				bool CanConvert
				(Type
				 	objectType)
			{
				return objectType == typeof (JObject);
			}
		}

		#endregion
	}
}