// -----------------------------------------------------------------------
//  <copyright file="RavenAwsClient.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Security.Cryptography;
using System.Text;
using Microsoft.AspNetCore.WebUtilities;
using Raven.Client.Util;

namespace Raven.Server.Documents.PeriodicBackup.Aws
{
    public abstract class RavenAwsClient : RavenStorageClient
    {
        public abstract string ServiceName { get; }

        public const string DefaultRegion = "us-east-1";

        private readonly string _awsAccessKey;
        private readonly byte[] _awsSecretKey;

        protected string AwsRegion { get; }

        protected RavenAwsClient(string awsAccessKey, string awsSecretKey, string awsRegionName)
        {
            awsRegionName = awsRegionName.ToLower();

            _awsAccessKey = awsAccessKey;
            _awsSecretKey = Encoding.UTF8.GetBytes("AWS4" + awsSecretKey);
            AwsRegion = awsRegionName;
        }

        public AuthenticationHeaderValue CalculateAuthorizationHeaderValue(HttpMethod httpMethod, string url, DateTime date, IDictionary<string, string> httpHeaders)
        {
            string signedHeaders;
            var canonicalRequestHash = CalculateCanonicalRequestHash(httpMethod, url, httpHeaders, out signedHeaders);
            var signingKey = CalculateSigningKey(date, ServiceName);

            using (var hash = new HMACSHA256(signingKey))
            {
                var scope = $"{date:yyyyMMdd}/{AwsRegion}/{ServiceName}/aws4_request";
                var stringToHash = $"AWS4-HMAC-SHA256\n{RavenAwsHelper.ConvertToString(date)}\n{scope}\n{canonicalRequestHash}";

                var hashedString = hash.ComputeHash(Encoding.UTF8.GetBytes(stringToHash));
                var signature = RavenAwsHelper.ConvertToHex(hashedString);

                var credentials = $"{_awsAccessKey}/{date:yyyyMMdd}/{AwsRegion}/{ServiceName}/aws4_request";

                return new AuthenticationHeaderValue("AWS4-HMAC-SHA256", $"Credential={credentials},SignedHeaders={signedHeaders},Signature={signature}");
            }
        }

        protected Dictionary<string, string> ConvertToHeaders(HttpHeaders headers, string name = null)
        {
            var result = headers.ToDictionary(x => x.Key, x => x.Value.FirstOrDefault());

            if(string.IsNullOrWhiteSpace(name) == false)
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

            var uri = new Uri(url);
            var canonicalUri = uri.AbsolutePath;

            var query = QueryHelpers.ParseQuery(uri.Query);
            var canonicalQueryString = query
                .OrderBy(parameter => parameter.Key)
                .Select(parameter => parameter.Value.Aggregate((current, value) => current + $"{parameter.Key}={value.Trim()}&"))
                .Aggregate(string.Empty, (current, parameter) => current + parameter);

            if (canonicalQueryString.EndsWith("&"))
                canonicalQueryString = canonicalQueryString.Substring(0, canonicalQueryString.Length - 1);

            var headers = httpHeaders
                .Where(x => isGet == false || x.Key.StartsWith("Date", StringComparison.OrdinalIgnoreCase) == false)
                .OrderBy(x => x.Key);

            var canonicalHeaders = headers
                .Aggregate(string.Empty, (current, parameter) => current + $"{parameter.Key.ToLower()}:{parameter.Value.Trim()}\n");

            signedHeaders = headers
                .Aggregate(string.Empty, (current, parameter) => current + parameter.Key.ToLower() + ";");

            if (signedHeaders.EndsWith(";"))
                signedHeaders = signedHeaders.Substring(0, signedHeaders.Length - 1);

            using (var hash = SHA256.Create())
            {
                var hashedPayload = httpHeaders["x-amz-content-sha256"];
                var canonicalRequest = $"{httpMethod}\n{canonicalUri}\n{canonicalQueryString}\n{canonicalHeaders}\n{signedHeaders}\n{hashedPayload}";

                return RavenAwsHelper.ConvertToHex(hash.ComputeHash(Encoding.UTF8.GetBytes(canonicalRequest)));
            }
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
    }
}