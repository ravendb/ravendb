// -----------------------------------------------------------------------
//  <copyright file="RavenAwsClient.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Reflection;
using System.Security.Cryptography;
using System.Text;
using System.Web.Http;

using Raven.Abstractions;
using Raven.Client.Extensions;
using System.Xml;

namespace Raven.Database.Client
{
	public class RavenAwsS3Client : RavenStorageClient
	{
		public const string DefaultRegion = "us-east-1";

		private static bool endpointsLoaded;

		private static readonly Dictionary<string, string> Endpoints = new Dictionary<string, string>();

		private readonly string awsAccessKey;

		private readonly byte[] awsSecretKey;

		private readonly string awsRegion;

		public RavenAwsS3Client(string awsAccessKey, string awsSecretKey, string awsRegionEndpoint)
		{
			this.awsAccessKey = awsAccessKey;
			this.awsSecretKey = Encoding.UTF8.GetBytes("AWS4" + awsSecretKey);

			awsRegion = GetAwsRegion(awsRegionEndpoint);
		}

		public void PutObject(string bucketName, string key, Stream stream, Dictionary<string, string> metadata, int timeoutInSeconds)
		{
			var url = GetUrl(bucketName, awsRegion) + "/" + key;

			var now = SystemTime.UtcNow;

			var payloadHash = CalculatePayloadHash(stream);

			var content = new StreamContent(stream)
			              {
				              Headers =
				              {
								  { "x-amz-date", ConvertToString(now) },
								  { "x-amz-content-sha256", payloadHash }
				              }
			              };

			foreach (var metadataKey in metadata.Keys)
				content.Headers.Add("x-amz-meta-" + metadataKey.ToLower(), metadata[metadataKey]);

			var headers = ConvertToHeaders(bucketName, awsRegion, content.Headers);

			var client = GetClient(TimeSpan.FromSeconds(timeoutInSeconds));
			var authorizationHeaderValue = CalculateAuthorizationHeaderValue("PUT", url, now, headers);
			client.DefaultRequestHeaders.Authorization = authorizationHeaderValue;

			var response = client.PutAsync(url, content).ResultUnwrap();
			if (response.IsSuccessStatusCode)
				return;

			throw new HttpResponseException(response);
		}

		public Blob GetObject(string bucketName, string key)
		{
			var url = GetUrl(bucketName, awsRegion) + "/" + key;

			var now = SystemTime.UtcNow;

			var payloadHash = CalculatePayloadHash(null);

			var requestMessage = new HttpRequestMessage(HttpMethod.Get, url)
			                     {
				                     Headers =
				                     {
										 { "x-amz-date", ConvertToString(now) },
										 { "x-amz-content-sha256", payloadHash }
				                     }
			                     };

			var headers = ConvertToHeaders(bucketName, awsRegion, requestMessage.Headers);

			var client = GetClient();
			client.DefaultRequestHeaders.Authorization = CalculateAuthorizationHeaderValue("GET", url, now, headers);

			var response = client.SendAsync(requestMessage).ResultUnwrap();
			if (response.StatusCode == HttpStatusCode.NotFound)
				return null;

			if (response.IsSuccessStatusCode == false)
				throw new HttpResponseException(response);

			var data = response.Content.ReadAsStreamAsync().ResultUnwrap();
			var metadataHeaders = response.Headers.ToDictionary(x => x.Key, x => x.Value.FirstOrDefault());

			return new Blob(data, metadataHeaders);
		}

		public string CalculatePayloadHash(Stream stream)
		{
			using (var hash = SHA256.Create())
			{
				var hashedPayload = ConvertToHex(stream != null ? hash.ComputeHash(stream) : hash.ComputeHash(Encoding.UTF8.GetBytes(string.Empty)));
				if (stream != null)
					stream.Position = 0;

				return hashedPayload;
			}
		}

		public AuthenticationHeaderValue CalculateAuthorizationHeaderValue(string httpMethod, string url, DateTime date, IDictionary<string, string> httpHeaders)
		{
			string signedHeaders;
			var canonicalRequestHash = CalculateCanonicalRequestHash(httpMethod, url, httpHeaders, out signedHeaders);
			var signingKey = CalculateSigningKey(date);

			using (var hash = new HMACSHA256(signingKey))
			{
				var scope = string.Format("{0}/{1}/s3/aws4_request", date.ToString("yyyyMMdd"), awsRegion);
				var stringToHash = string.Format("AWS4-HMAC-SHA256\n{0}\n{1}\n{2}", ConvertToString(date), scope, canonicalRequestHash);

				var hashedString = hash.ComputeHash(Encoding.UTF8.GetBytes(stringToHash));
				var signature = ConvertToHex(hashedString);

				var credentials = string.Format("{0}/{1}/{2}/s3/aws4_request", awsAccessKey, date.ToString("yyyyMMdd"), awsRegion);

				return new AuthenticationHeaderValue("AWS4-HMAC-SHA256", string.Format("Credential={0},SignedHeaders={1},Signature={2}", credentials, signedHeaders, signature));
			}
		}

		private static string CalculateCanonicalRequestHash(string httpMethod, string url, IDictionary<string, string> httpHeaders, out string signedHeaders)
		{
			var httpMethodToUpper = httpMethod.ToUpper();
			var isGet = httpMethodToUpper == "GET";

			var uri = new Uri(url);
			var queryStringCollection = uri.ParseQueryString();

			var canonicalUri = uri.AbsolutePath;

			var queryString = (
				from string parameter in queryStringCollection 
				select new KeyValuePair<string, string>(parameter, queryStringCollection.Get(parameter))
				);

			var canonicalQueryString = queryString
				.OrderBy(x => x.Key)
				.Aggregate(string.Empty, (current, parameter) => current + string.Format("{0}={1}&", parameter.Key.ToLower(), parameter.Value.Trim()));

			if (canonicalQueryString.EndsWith("&"))
				canonicalQueryString = canonicalQueryString.Substring(0, canonicalQueryString.Length - 1);

			var headers = httpHeaders
				.Where(x => isGet == false || x.Key.StartsWith("Date", StringComparison.InvariantCultureIgnoreCase) == false)
				.OrderBy(x => x.Key);

			var canonicalHeaders = headers
				.Aggregate(string.Empty, (current, parameter) => current + string.Format("{0}:{1}\n", parameter.Key.ToLower(), parameter.Value.Trim()));

			signedHeaders = headers
				.Aggregate(string.Empty, (current, parameter) => current + parameter.Key.ToLower() + ";");

			if (signedHeaders.EndsWith(";"))
				signedHeaders = signedHeaders.Substring(0, signedHeaders.Length - 1);

			using (var hash = SHA256.Create())
			{
				var hashedPayload = httpHeaders["x-amz-content-sha256"];
				var canonicalRequest = string.Format("{0}\n{1}\n{2}\n{3}\n{4}\n{5}", httpMethodToUpper, canonicalUri, canonicalQueryString, canonicalHeaders, signedHeaders, hashedPayload);

				return ConvertToHex(hash.ComputeHash(Encoding.UTF8.GetBytes(canonicalRequest)));
			}
		}

		private byte[] CalculateSigningKey(DateTime date)
		{
			byte[] key;
			using (var hash = new HMACSHA256(awsSecretKey))
				key = hash.ComputeHash(Encoding.UTF8.GetBytes(date.ToString("yyyyMMdd")));

			using (var hash = new HMACSHA256(key))
				key = hash.ComputeHash(Encoding.UTF8.GetBytes(awsRegion));

			using (var hash = new HMACSHA256(key))
				key = hash.ComputeHash(Encoding.UTF8.GetBytes("s3"));

			using (var hash = new HMACSHA256(key))
				return hash.ComputeHash(Encoding.UTF8.GetBytes("aws4_request"));
		}

		private string GetAwsRegion(string awsRegionEndpoint)
		{
			string endpoint;
			if (Endpoints.TryGetValue(awsRegionEndpoint.ToLower(), out endpoint))
				return endpoint;

			if (endpointsLoaded)
				throw new InvalidOperationException("Given endpoint is invalid: " + awsRegionEndpoint);

			LoadEndpoints();

			return GetAwsRegion(awsRegionEndpoint);
		}

		private void LoadEndpoints()
		{
			if (endpointsLoaded)
				return;

			Endpoints.Clear();

			var response = GetClient().GetAsync("http://aws-sdk-configurations.amazonwebservices.com/endpoints.xml").ResultUnwrap();
			if (response.IsSuccessStatusCode)
			{
				using (var stream = response.Content.ReadAsStreamAsync().ResultUnwrap()) 
				using (var reader = new StreamReader(stream)) 
					LoadEndpointsFromReader(reader);

				return;
			}

			using (var stream = Assembly.GetExecutingAssembly().GetManifestResourceStream("Raven.Database.Client.Amazon.AWS.endpoints.xml"))
			using (var reader = new StreamReader(stream))
				LoadEndpointsFromReader(reader);
		}

		private static void LoadEndpointsFromReader(TextReader reader)
		{
			var document = new XmlDocument();
			document.Load(reader);

			foreach (XmlElement node in document.SelectNodes("//Regions/Region"))
			{
				var nodeName = node["Name"].InnerText.ToLower();
				Endpoints.Add(nodeName, nodeName);
			}

			endpointsLoaded = true;
		}

		private static string ConvertToHex(byte[] array)
		{
			return BitConverter.ToString(array).Replace("-", "").ToLower();
		}

		private static Dictionary<string, string> ConvertToHeaders(string bucketName, string awsRegion, HttpHeaders headers)
		{
			var result = headers.ToDictionary(x => x.Key, x => x.Value.FirstOrDefault());
			result.Add("Host", GetHost(bucketName, awsRegion));

			return result;
		}

		public static string GetUrl(string bucketName, string awsRegion)
		{
			return "https://" + GetHost(bucketName, awsRegion);
		}

		public static string GetHost(string bucketName, string awsRegion)
		{
			if (awsRegion == "us-east-1")
				return string.Format("{0}.s3.amazonaws.com", bucketName);

			return string.Format("{0}.s3-{1}.amazonaws.com", bucketName, awsRegion);
		}

		public static string ConvertToString(DateTime date)
		{
			return date.ToString("yyyyMMddTHHmmssZ");
		}
	}
}