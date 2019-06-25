// -----------------------------------------------------------------------
//  <copyright file="RavenAwsClient.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using Microsoft.AspNetCore.WebUtilities;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Util;

namespace Raven.Server.Documents.PeriodicBackup.Aws
{
    public abstract class RavenAwsClient : RavenStorageClient
    {
        protected const string DefaultRegion = "us-east-1";
        protected bool IsRegionInvariantRequest;
        public abstract string ServiceName { get; }

        private readonly string _awsAccessKey;
        private readonly byte[] _awsSecretKey;
        private readonly string _sessionToken;

        protected string AwsRegion { get; set; }

        public string RemoteFolderName { get; }

        protected RavenAwsClient(AmazonSettings amazonSettings, Progress progress, CancellationToken? cancellationToken = null)
            : base(progress, cancellationToken)
        {
            if (string.IsNullOrWhiteSpace(amazonSettings.AwsAccessKey))
                throw new ArgumentException("AWS access key cannot be null or empty");  
            
            if (string.IsNullOrWhiteSpace(amazonSettings.AwsSecretKey))
                throw new ArgumentException("AWS secret key cannot be null or empty");

            if (string.IsNullOrWhiteSpace(amazonSettings.AwsRegionName))
                throw new ArgumentException("AWS region cannot be null or empty");

            _awsAccessKey = amazonSettings.AwsAccessKey;
            _awsSecretKey = Encoding.UTF8.GetBytes("AWS4" + amazonSettings.AwsSecretKey);
            _sessionToken = amazonSettings.AwsSessionToken;
            AwsRegion = amazonSettings.AwsRegionName.ToLower();
            RemoteFolderName = amazonSettings.RemoteFolderName;
        }

        public AuthenticationHeaderValue CalculateAuthorizationHeaderValue(HttpMethod httpMethod, string url, DateTime date, IDictionary<string, string> httpHeaders)
        {
            var canonicalRequestHash = CalculateCanonicalRequestHash(httpMethod, url, httpHeaders, out string signedHeaders);
            var signingKey = CalculateSigningKey(date, ServiceName);

            using (var hash = new HMACSHA256(signingKey))
            {
                var regionForRequest = IsRegionInvariantRequest ? DefaultRegion : AwsRegion;
                var scope = $"{date.ToString("yyyyMMdd", CultureInfo.InvariantCulture)}/{regionForRequest}/{ServiceName}/aws4_request";
                var stringToHash = $"AWS4-HMAC-SHA256\n{RavenAwsHelper.ConvertToString(date)}\n{scope}\n{canonicalRequestHash}";

                var hashedString = hash.ComputeHash(Encoding.UTF8.GetBytes(stringToHash));
                var signature = RavenAwsHelper.ConvertToHex(hashedString);

                var credentials = $"{_awsAccessKey}/{date.ToString("yyyyMMdd", CultureInfo.InvariantCulture)}/{regionForRequest}/{ServiceName}/aws4_request";

                return new AuthenticationHeaderValue("AWS4-HMAC-SHA256", $"Credential={credentials},SignedHeaders={signedHeaders},Signature={signature}");
            }
        }

        protected Dictionary<string, string> ConvertToHeaders(HttpHeaders headers)
        {
            var result = headers.ToDictionary(x => x.Key, x => x.Value.FirstOrDefault());
            result.Add("Host", GetHost());
            return result;
        }

        public virtual string GetUrl()
        {
            return "https://" + GetHost();
        }

        public abstract string GetHost();

        protected virtual void UpdateHeaders(HttpHeaders headers, DateTime now, Stream stream, string payloadHash = null)
        {
            headers.Add("x-amz-date", RavenAwsHelper.ConvertToString(now));
            headers.Add("x-amz-content-sha256", payloadHash ?? RavenAwsHelper.CalculatePayloadHash(stream));

            if (_sessionToken != null)
                headers.Add("x-amz-security-token", _sessionToken);
        }

        private static string CalculateCanonicalRequestHash(HttpMethod httpMethod, string url, IDictionary<string, string> httpHeaders, out string signedHeaders)
        {
            var isGet = httpMethod == HttpMethods.Get;

            var uri = new Uri(url);
            var canonicalUri = uri.AbsolutePath;

            var query = QueryHelpers.ParseQuery(uri.Query);
            var canonicalQueryString = query
                .OrderBy(parameter => parameter.Key)
                .Select(parameter => parameter.Value.Aggregate(string.Empty, (current, value) =>
                    current + $"{parameter.Key}={Uri.EscapeDataString(value.Trim())}&"))
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
                key = hash.ComputeHash(Encoding.UTF8.GetBytes(date.ToString("yyyyMMdd", CultureInfo.InvariantCulture)));

            var regionForRequest = IsRegionInvariantRequest ? DefaultRegion : AwsRegion;
            using (var hash = new HMACSHA256(key))
                key = hash.ComputeHash(Encoding.UTF8.GetBytes(regionForRequest));

            using (var hash = new HMACSHA256(key))
                key = hash.ComputeHash(Encoding.UTF8.GetBytes(service));

            using (var hash = new HMACSHA256(key))
                return hash.ComputeHash(Encoding.UTF8.GetBytes("aws4_request"));
        }
    }
}
