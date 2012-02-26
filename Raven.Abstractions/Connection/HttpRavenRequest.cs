using System;
using System.IO;
using System.Net;
using Newtonsoft.Json;
using Newtonsoft.Json.Bson;
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
			request.Headers["Accept-Encoding"] = "deflate,gzip";
			request.ContentType = "application/json; charset=utf-8";
			request.UseDefaultCredentials = true;
			request.PreAuthenticate = true;
			configureRequest(connectionStringOptions, request);
			return request;
		}

		public void Write(Stream streamToWrite)
		{
			postedStream = streamToWrite;
			WebRequest.ContentLength = streamToWrite.Length;
			using (var stream = WebRequest.GetRequestStream())
			{
				streamToWrite.CopyTo(stream);
				stream.Flush();
			}
		}

		public long Write(RavenJToken ravenJToken)
		{
			postedToken = ravenJToken;
			return WriteToken(WebRequest);
		}

		public long Write(byte[] data)
		{
			postedData = data;
			webRequest.ContentLength = data.Length;
			using (var stream = WebRequest.GetRequestStream())
			{
				stream.Write(data, 0, data.Length);
				stream.Flush();
				return stream.CanSeek ? stream.Length : 0;
			}
		}

		public void WriteBson(RavenJToken ravenJToken)
		{
			writeBson = true;
			postedToken = ravenJToken;
			WriteToken(WebRequest);
		}

		private long WriteToken(WebRequest httpWebRequest)
		{
			using (var stream = httpWebRequest.GetRequestStream())
			{
				if (writeBson)
				{
					postedToken.WriteTo(new BsonWriter(stream));
				}
				else
				{
					using (var streamWriter = new StreamWriter(stream))
					{
						postedToken.WriteTo(new JsonTextWriter(streamWriter));
						streamWriter.Flush();
					}
				}
				stream.Flush();
				return stream.CanSeek ? stream.Length : 0;
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
					if (response == null ||
						response.StatusCode != HttpStatusCode.Unauthorized)
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
				{
					postedStream.CopyTo(stream);
					stream.Flush();
				}
			}
			WebRequest = newWebRequest;
		}

	}
}