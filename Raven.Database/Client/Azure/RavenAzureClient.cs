// -----------------------------------------------------------------------
//  <copyright file="RavenAzureClient.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;

using Raven.Abstractions;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Util;
using Raven.Client.Extensions;

namespace Raven.Database.Client.Azure
{
	public class RavenAzureClient : RavenStorageClient
	{
		private readonly string accountName;

		private readonly byte[] accountKey;


		public RavenAzureClient(string accountName, string accountKey)
		{
			this.accountName = accountName;
			this.accountKey = Convert.FromBase64String(accountKey);
		}

		public void PutContainer(string containerName)
		{
			var url = GetUrl(containerName) + "?restype=container";

			var now = SystemTime.UtcNow;
			var content = new EmptyContent
						  {
							  Headers =
					              {
						              {"x-ms-date", now.ToString("R") },
									  {"x-ms-version", "2011-08-18" },
					              }
						  };

			var client = GetClient();
			client.DefaultRequestHeaders.Authorization = CalculateAuthorizationHeaderValue("PUT", url, content.Headers);

			var response = AsyncHelpers.RunSync(() => client.PutAsync(url, content));
			if (response.IsSuccessStatusCode)
				return;

			if (response.StatusCode == HttpStatusCode.Conflict)
				return;

			throw ErrorResponseException.FromResponseMessage(response);
		}

		public void PutBlob(string containerName, string key, Stream stream, Dictionary<string, string> metadata)
		{
			var url = GetUrl(containerName) + "/" + key;

			var now = SystemTime.UtcNow;
			var content = new StreamContent(stream)
						  {
							  Headers =
				              {
					              { "x-ms-date", now.ToString("R") }, 
								  { "x-ms-version", "2011-08-18" },
								  { "x-ms-blob-type", "BlockBlob" },
								  { "Content-Length", stream.Length.ToString(CultureInfo.InvariantCulture) }
				              }
						  };

			foreach (var metadataKey in metadata.Keys)
				content.Headers.Add("x-ms-meta-" + metadataKey.ToLower(), metadata[metadataKey]);

			var client = GetClient(TimeSpan.FromHours(1));
			client.DefaultRequestHeaders.Authorization = CalculateAuthorizationHeaderValue("PUT", url, content.Headers);

			var response = AsyncHelpers.RunSync(() => client.PutAsync(url, content));

			if (response.IsSuccessStatusCode)
				return;

			throw ErrorResponseException.FromResponseMessage(response);
		}

		public Blob GetBlob(string containerName, string key)
		{
			var url = GetUrl(containerName) + "/" + key;

			var now = SystemTime.UtcNow;

			var requestMessage = new HttpRequestMessage(HttpMethod.Get, url)
			                     {
				                     Headers =
				                     {
					                     { "x-ms-date", now.ToString("R") }, 
										 { "x-ms-version", "2011-08-18" }
				                     }
			                     };

			var client = GetClient();
			client.DefaultRequestHeaders.Authorization = CalculateAuthorizationHeaderValue("GET", url, requestMessage.Headers);

			var response = AsyncHelpers.RunSync(() => client.SendAsync(requestMessage));
			if (response.StatusCode == HttpStatusCode.NotFound) 
				return null;

			if (response.IsSuccessStatusCode == false)
				throw ErrorResponseException.FromResponseMessage(response);

			var data = AsyncHelpers.RunSync(() => response.Content.ReadAsStreamAsync());
			var headers = response.Headers.ToDictionary(x => x.Key, x => x.Value.FirstOrDefault());

			return new Blob(data, headers);
		}

		private string GetUrl(string containerName)
		{
			return string.Format("https://{0}.blob.core.windows.net/{1}", accountName, containerName.ToLower());
		}

		private AuthenticationHeaderValue CalculateAuthorizationHeaderValue(string httpMethod, string url, HttpHeaders httpHeaders)
		{
			var stringToHash = ComputeCanonicalizedHeaders(httpMethod, httpHeaders);
			stringToHash += ComputeCanonicalizedResource(url);

			if (stringToHash.EndsWith("\n"))
				stringToHash = stringToHash.Substring(0, stringToHash.Length - 1);

			using (var hash = new HMACSHA256(accountKey))
			{
				var hashedString = hash.ComputeHash(Encoding.UTF8.GetBytes(stringToHash));
				var base64String = Convert.ToBase64String(hashedString);

				return new AuthenticationHeaderValue("SharedKey", string.Format("{0}:{1}", accountName, base64String));
			}
		}

		private static string ComputeCanonicalizedHeaders(string httpMethod, HttpHeaders httpHeaders)
		{
			var headers = httpHeaders
				.Where(x => x.Key.StartsWith("x-ms-"))
				.OrderBy(x => x.Key);

			var httpMethodToUpper = httpMethod.ToUpper();

			var contentLength = httpMethodToUpper == "GET" ? string.Empty : "0";
			IEnumerable<string> values;
			if (httpHeaders.TryGetValues("Content-Length", out values))
				contentLength = values.First();

			var stringToHash = string.Format("{0}\n\n\n{1}\n\n\n\n\n\n\n\n\n", httpMethodToUpper, contentLength);

			return headers.Aggregate(stringToHash, (current, header) => current + string.Format("{0}:{1}\n", header.Key.ToLower(), header.Value.First()));
		}

		private string ComputeCanonicalizedResource(string url)
		{
			var uri = new Uri(url, UriKind.Absolute);

			var stringToHash = string.Format("/{0}{1}\n", accountName, uri.AbsolutePath);
			var queryStringCollection = uri.ParseQueryString();

			var queryString = (
				from string parameter in queryStringCollection 
				select new KeyValuePair<string, string>(parameter, queryStringCollection.Get(parameter))
				);

			return queryString
				.OrderBy(x => x.Key)
				.Aggregate(stringToHash, (current, parameter) => current + string.Format("{0}:{1}\n", parameter.Key.ToLower(), parameter.Value));
		}

		private class EmptyContent : HttpContent
		{
			protected override Task SerializeToStreamAsync(Stream stream, TransportContext context)
			{
				return new CompletedTask();
			}

			protected override bool TryComputeLength(out long length)
			{
				length = 0;
				return true;
			}
		}
	}
}