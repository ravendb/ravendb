using System;
using System.IO;
using System.IO.Compression;
using System.Net;
using Raven.Imports.Newtonsoft.Json;
using Raven.Imports.Newtonsoft.Json.Bson;
using Raven.Abstractions.Data;
using Raven.Json.Linq;
using Raven.Abstractions.Extensions;

namespace Raven.Abstractions.Connection
{
	public class HttpRavenRequest
	{
		private readonly string url;
		private readonly string method;
		private readonly Action<RavenConnectionStringOptions, WebRequest> configureRequest;
		private readonly Func<RavenConnectionStringOptions, WebResponse, bool> handleUnauthorizedResponse;
		private readonly RavenConnectionStringOptions connectionStringOptions;

		private HttpWebRequest webRequest;

		private Stream postedStream;
		private RavenJToken postedToken;
		private byte[] postedData;
		private bool writeBson;

		public long NumberOfBytesWrittenCompressed { get; private set; }
		public long NumberOfBytesWrittenUncompressed { get; private set; }

		public HttpWebRequest WebRequest
		{
			get { return webRequest ?? (webRequest = CreateRequest()); }
			set { webRequest = value; }
		}

		public HttpRavenRequest(string url, string method, Action<RavenConnectionStringOptions, WebRequest> configureRequest, Func<RavenConnectionStringOptions, WebResponse, bool> handleUnauthorizedResponse, RavenConnectionStringOptions connectionStringOptions)
		{
			this.url = url;
			this.method = method;
			this.configureRequest = configureRequest;
			this.handleUnauthorizedResponse = handleUnauthorizedResponse;
			this.connectionStringOptions = connectionStringOptions;
		}

		private HttpWebRequest CreateRequest()
		{
			var request = (HttpWebRequest) System.Net.WebRequest.Create(url);
			request.Method = method;
			if (method == "POST" || method == "PUT")
				request.Headers["Content-Encoding"] = "gzip";
			request.Headers["Accept-Encoding"] = "deflate,gzip";
			request.ContentType = "application/json; charset=utf-8";

			if (connectionStringOptions.Credentials != null)
				request.Credentials = connectionStringOptions.Credentials;
			else
				request.UseDefaultCredentials = true;

			configureRequest(connectionStringOptions, request);
			return request;
		}

		public void Write(Stream streamToWrite)
		{
			postedStream = streamToWrite;
			using (var stream = WebRequest.GetRequestStream())
			using(var countingStream = new CountingStream(stream, l => NumberOfBytesWrittenCompressed = l))
			using(var commpressedStream = new GZipStream(countingStream, CompressionMode.Compress))
			using (var countingStream2 = new CountingStream(commpressedStream, l => NumberOfBytesWrittenUncompressed = l))
			{
				streamToWrite.CopyTo(countingStream2);
				commpressedStream.Flush();
				stream.Flush();
			}
		}

		public void Write(RavenJToken ravenJToken)
		{
			postedToken = ravenJToken;
			WriteToken(WebRequest);
		}

		public void Write(byte[] data)
		{
			postedData = data;
			using (var stream = WebRequest.GetRequestStream())
			using (var countingStream = new CountingStream(stream, l => NumberOfBytesWrittenCompressed = l))
			using (var cmp = new GZipStream(countingStream, CompressionMode.Compress))
			using (var countingStream2 = new CountingStream(cmp, l => NumberOfBytesWrittenUncompressed = l))
			{
				countingStream2.Write(data, 0, data.Length);
				cmp.Flush();
				stream.Flush();
			}
		}

		public void WriteBson(RavenJToken ravenJToken)
		{
			writeBson = true;
			postedToken = ravenJToken;
			WriteToken(WebRequest);
		}

		private void WriteToken(WebRequest httpWebRequest)
		{
			using (var stream = httpWebRequest.GetRequestStream())
			using (var countingStream = new CountingStream(stream, l => NumberOfBytesWrittenCompressed = l))
			using (var commpressedData = new GZipStream(countingStream, CompressionMode.Compress))
			using (var countingStream2 = new CountingStream(commpressedData, l => NumberOfBytesWrittenUncompressed = l))
			{
				if (writeBson)
				{
					postedToken.WriteTo(new BsonWriter(countingStream2));
				}
				else
				{
					var streamWriter = new StreamWriter(countingStream2);
					postedToken.WriteTo(new JsonTextWriter(streamWriter));
					streamWriter.Flush();
				}
				commpressedData.Flush();
				stream.Flush();
			}
		}

		public T ExecuteRequest<T>()
		{
			T result = default(T);
			SendRequestToServer(response =>
			                    	{
			                    		using (var stream = response.GetResponseStreamWithHttpDecompression())
			                    		using (var reader = new StreamReader(stream))
			                    		{
			                    			result = reader.JsonDeserialization<T>();
			                    		}
			                    	});
			return result;
		}

		public void ExecuteRequest(Action<StreamReader> action)
		{
			SendRequestToServer(response =>
			{
				using (var stream = response.GetResponseStreamWithHttpDecompression())
				using (var reader = new StreamReader(stream))
				{
					action(reader);
				}
			});
		}

		public void ExecuteRequest(Action<Stream> action)
		{
			SendRequestToServer(response =>
			{
				using (var stream = response.GetResponseStreamWithHttpDecompression())
				{
					action(stream);
				}
			});
		}

		public void ExecuteRequest()
		{
			SendRequestToServer(response => { });
		}

		private void SendRequestToServer(Action<WebResponse> action)
		{
			int retries = 0;
			while (true)
			{
				try
				{
					using (var res = WebRequest.GetResponse())
					{
						action(res);
					}
					return;
				}
				catch (WebException e)
				{
					if (++retries >= 3)
						throw;

					var response = e.Response as HttpWebResponse;
					if (response == null)
						throw;

					if (response.StatusCode != HttpStatusCode.Unauthorized)
					{
						using (var streamReader = new StreamReader(response.GetResponseStreamWithHttpDecompression()))
						{
							var error = streamReader.ReadToEnd();
							var ravenJObject = RavenJObject.Parse(error);
							throw new WebException("Error: " + ravenJObject.Value<string>("Error"), e);
						}
					}

					if (handleUnauthorizedResponse != null && handleUnauthorizedResponse(connectionStringOptions, e.Response))
					{
						RecreateWebRequest();
					}
					else
					{
						throw;
					}
				}
			}
		}

		private void RecreateWebRequest()
		{
			// we now need to clone the request, since just calling GetRequest again wouldn't do anything
			var newWebRequest = CreateRequest();
			HttpRequestHelper.CopyHeaders(WebRequest, newWebRequest);

			if (postedToken != null)
			{
				WriteToken(newWebRequest);
			}
			if (postedData != null)
			{
				Write(postedData);
			}
			if (postedStream != null)
			{
				postedStream.Position = 0;
				using (var stream = newWebRequest.GetRequestStream())	
				using (var compressedData = new GZipStream(stream, CompressionMode.Compress))
				{
					postedStream.CopyTo(compressedData);
					stream.Flush();
					compressedData.Flush();
				}
			}
			WebRequest = newWebRequest;
		}

	}
}