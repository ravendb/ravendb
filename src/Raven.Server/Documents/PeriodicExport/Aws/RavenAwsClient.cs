// -----------------------------------------------------------------------
//  <copyright file="RavenAwsClient.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Reflection;
using System.Security.Cryptography;
using System.Text;
using System.Xml;
using Raven.Abstractions.Util;

namespace Raven.Server.Documents.PeriodicExport.Aws
{
    public abstract class RavenAwsClient : RavenStorageClient
    {
        public abstract string ServiceName { get; }

        public const string DefaultRegion = "us-east-1";

        private static bool _endpointsLoaded;

        private static readonly Dictionary<string, string> Endpoints = new Dictionary<string, string>();

        private readonly string _awsAccessKey;

        private readonly byte[] _awsSecretKey;

        protected string AwsRegion { get; private set; }

        protected RavenAwsClient(string awsAccessKey, string awsSecretKey, string awsRegionEndpoint)
        {
            this._awsAccessKey = awsAccessKey;
            this._awsSecretKey = Encoding.UTF8.GetBytes("AWS4" + awsSecretKey);

            AwsRegion = GetAwsRegion(awsRegionEndpoint);
        }

        public AuthenticationHeaderValue CalculateAuthorizationHeaderValue(HttpMethod httpMethod, string url, DateTime date, IDictionary<string, string> httpHeaders)
        {
            string signedHeaders;
            var canonicalRequestHash = CalculateCanonicalRequestHash(httpMethod, url, httpHeaders, out signedHeaders);
            var signingKey = CalculateSigningKey(date, ServiceName);

            using (var hash = new HMACSHA256(signingKey))
            {
                var scope = string.Format("{0}/{1}/{2}/aws4_request", date.ToString("yyyyMMdd"), AwsRegion, ServiceName);
                var stringToHash = string.Format("AWS4-HMAC-SHA256\n{0}\n{1}\n{2}", RavenAwsHelper.ConvertToString(date), scope, canonicalRequestHash);

                var hashedString = hash.ComputeHash(Encoding.UTF8.GetBytes(stringToHash));
                var signature = RavenAwsHelper.ConvertToHex(hashedString);

                var credentials = string.Format("{0}/{1}/{2}/{3}/aws4_request", _awsAccessKey, date.ToString("yyyyMMdd"), AwsRegion, ServiceName);

                return new AuthenticationHeaderValue("AWS4-HMAC-SHA256", string.Format("Credential={0},SignedHeaders={1},Signature={2}", credentials, signedHeaders, signature));
            }
        }

        protected Dictionary<string, string> ConvertToHeaders(string name, HttpHeaders headers)
        {
            var result = headers.ToDictionary(x => x.Key, x => x.Value.FirstOrDefault());
            result.Add("Host", GetHost(name));

            return result;
        }

        public string GetUrl(string name)
        {
            return "https://" + GetHost(name);
        }

        public abstract string GetHost(string bucketName);

        private static string CalculateCanonicalRequestHash(HttpMethod httpMethod, string url, IDictionary<string, string> httpHeaders, out string signedHeaders)
        {
            var isGet = httpMethod == HttpMethods.Get;
            throw new NotImplementedException("Fitzchak: please fix");
            /*var uri = new Uri(url);
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
                var canonicalRequest = string.Format("{0}\n{1}\n{2}\n{3}\n{4}\n{5}", httpMethod, canonicalUri, canonicalQueryString, canonicalHeaders, signedHeaders, hashedPayload);

                return RavenAwsHelper.ConvertToHex(hash.ComputeHash(Encoding.UTF8.GetBytes(canonicalRequest)));
            }*/
        }

        private byte[] CalculateSigningKey(DateTime date, string service)
        {
            byte[] key;
            using (var hash = new HMACSHA256(_awsSecretKey))
                key = hash.ComputeHash(Encoding.UTF8.GetBytes(date.ToString("yyyyMMdd")));

            using (var hash = new HMACSHA256(key))
                key = hash.ComputeHash(Encoding.UTF8.GetBytes(AwsRegion));

            using (var hash = new HMACSHA256(key))
                key = hash.ComputeHash(Encoding.UTF8.GetBytes(service));

            using (var hash = new HMACSHA256(key))
                return hash.ComputeHash(Encoding.UTF8.GetBytes("aws4_request"));
        }

        private string GetAwsRegion(string awsRegionEndpoint)
        {
            string endpoint;
            if (Endpoints.TryGetValue(awsRegionEndpoint.ToLower(), out endpoint))
                return endpoint;

            if (_endpointsLoaded)
                throw new InvalidOperationException("Given endpoint is invalid: " + awsRegionEndpoint);

            LoadEndpoints();

            return GetAwsRegion(awsRegionEndpoint);
        }

        private void LoadEndpoints()
        {
            if (_endpointsLoaded)
                return;

            Endpoints.Clear();

            var response = AsyncHelpers.RunSync(() => GetClient().GetAsync("http://aws-sdk-configurations.amazonwebservices.com/endpoints.xml"));
            if (response.IsSuccessStatusCode)
            {
                using (var stream = AsyncHelpers.RunSync(() => response.Content.ReadAsStreamAsync()))
                using (var reader = new StreamReader(stream))
                    LoadEndpointsFromReader(reader);

                return;
            }

            using (var stream = typeof(RavenAwsClient).GetTypeInfo().Assembly.GetManifestResourceStream("Raven.Server.Documents.PeriodicExport.Aws.Amazon.AWS.endpoints.xml"))
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

            _endpointsLoaded = true;
        }
    }
}