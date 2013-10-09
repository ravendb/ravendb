using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading.Tasks;
using Raven.Abstractions.Connection;
using System.Linq;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Util;
#if SILVERLIGHT
using System.Net.Browser;
using Raven.Client.Changes;

#endif
using Raven.Client.Connection;

namespace Raven.Abstractions.OAuth
{
	public class SecuredAuthenticator : AbstractAuthenticator
	{
		public SecuredAuthenticator(string apiKey) : base(apiKey)
		{
		}

		public override void ConfigureRequest(object sender, WebRequestEventArgs e)
		{
			if (CurrentOauthToken != null)
			{
				base.ConfigureRequest(sender, e);
				return;
			}
			if (ApiKey != null)
			{
#if NETFX_CORE || SILVERLIGHT
				e.Client.DefaultRequestHeaders.Add("Has-Api-Key", "true");
#else
				e.Request.Headers["Has-Api-Key"] = "true";
#endif
			}
		}

		private Tuple<HttpWebRequest, string> PrepareOAuthRequest(string oauthSource, string serverRSAExponent, string serverRSAModulus, string challenge)
		{
#if !SILVERLIGHT
			var authRequest = (HttpWebRequest)WebRequest.Create(oauthSource);
#else
			var authRequest = (HttpWebRequest)WebRequestCreator.ClientHttp.Create(new Uri(oauthSource));
#endif
			authRequest.Headers["grant_type"] = "client_credentials";
			authRequest.Accept = "application/json;charset=UTF-8";
			authRequest.Method = "POST";

			if (!string.IsNullOrEmpty(serverRSAExponent) && !string.IsNullOrEmpty(serverRSAModulus) && !string.IsNullOrEmpty(challenge))
			{
				var exponent = OAuthHelper.ParseBytes(serverRSAExponent);
				var modulus = OAuthHelper.ParseBytes(serverRSAModulus);

				var apiKeyParts = ApiKey.Split(new[] { '/' }, StringSplitOptions.None);

				if (apiKeyParts.Length > 2)
				{
					apiKeyParts[1] = string.Join("/", apiKeyParts.Skip(1));
				}

				if (apiKeyParts.Length < 2)
					throw new InvalidOperationException("Invalid API key");

				var apiKeyName = apiKeyParts[0].Trim();
				var apiSecret = apiKeyParts[1].Trim();


				var data = OAuthHelper.DictionaryToString(new Dictionary<string, string>
				{
					{OAuthHelper.Keys.RSAExponent, serverRSAExponent},
					{OAuthHelper.Keys.RSAModulus, serverRSAModulus},
					{
						OAuthHelper.Keys.EncryptedData,
						OAuthHelper.EncryptAsymmetric(exponent, modulus, OAuthHelper.DictionaryToString(new Dictionary<string, string>
						{
							{OAuthHelper.Keys.APIKeyName, apiKeyName},
							{OAuthHelper.Keys.Challenge, challenge},
							{OAuthHelper.Keys.Response, OAuthHelper.Hash(string.Format(OAuthHelper.Keys.ResponseFormat, challenge, apiSecret))}
						}))
					}
				});

				return Tuple.Create(authRequest, data);
			}
#if !NETFX_CORE
			authRequest.ContentLength = 0;
#endif
			return Tuple.Create(authRequest, (string)null);
		}


#if !SILVERLIGHT && !NETFX_CORE
		public override Action<HttpWebRequest> DoOAuthRequest(string oauthSource)
		{
			string serverRSAExponent = null;
			string serverRSAModulus = null;
			string challenge = null;

			// Note that at two tries will be needed in the normal case.
			// The first try will get back a challenge,
			// the second try will try authentication. If something goes wrong server-side though
			// (e.g. the server was just rebooted or the challenge timed out for some reason), we
			// might get a new challenge back, so we try a third time just in case.
			int tries = 0;
			while (true)
			{
				tries++;
				var authRequestTuple = PrepareOAuthRequest(oauthSource, serverRSAExponent, serverRSAModulus, challenge);
				var authRequest = authRequestTuple.Item1;
				if (authRequestTuple.Item2 != null)
				{
					using (var stream = authRequest.GetRequestStream())
					using (var writer = new StreamWriter(stream))
					{
						writer.Write(authRequestTuple.Item2);
					}
				}

				try
				{
					using (var authResponse = authRequest.GetResponse())
					using (var stream = authResponse.GetResponseStreamWithHttpDecompression())
					using (var reader = new StreamReader(stream))
					{
						CurrentOauthToken = "Bearer " + reader.ReadToEnd();
						return (Action<HttpWebRequest>)(request => SetHeader(request.Headers, "Authorization", CurrentOauthToken));
					}
				}
				catch (WebException ex)
				{
					if (tries > 2)
						// We've already tried three times and failed
						throw;

					var authResponse = ex.Response as HttpWebResponse;
					if (authResponse == null || authResponse.StatusCode != HttpStatusCode.PreconditionFailed)
						throw;

					var header = authResponse.Headers[HttpResponseHeader.WwwAuthenticate];
					if (string.IsNullOrEmpty(header) || !header.StartsWith(OAuthHelper.Keys.WWWAuthenticateHeaderKey))
						throw;

					authResponse.Close();

					var challengeDictionary = OAuthHelper.ParseDictionary(header.Substring(OAuthHelper.Keys.WWWAuthenticateHeaderKey.Length).Trim());
					serverRSAExponent = challengeDictionary.GetOrDefault(OAuthHelper.Keys.RSAExponent);
					serverRSAModulus = challengeDictionary.GetOrDefault(OAuthHelper.Keys.RSAModulus);
					challenge = challengeDictionary.GetOrDefault(OAuthHelper.Keys.Challenge);

					if (string.IsNullOrEmpty(serverRSAExponent) || string.IsNullOrEmpty(serverRSAModulus) || string.IsNullOrEmpty(challenge))
					{
						throw new InvalidOperationException("Invalid response from server, could not parse raven authentication information: " + header);
					}
				}
			}
		}
#endif

		public Task<Action<HttpClient>> DoOAuthRequestAsync(string baseUrl, string oauthSource)
		{
			return DoOAuthRequestAsync(baseUrl, oauthSource, null, null, null, 0);
		}

		private async Task<Action<HttpClient>> DoOAuthRequestAsync(string baseUrl, string oauthSource, string serverRsaExponent, string serverRsaModulus, string challenge, int tries)
		{
			if (oauthSource == null)
				throw new ArgumentNullException("oauthSource");

			var httpClient = new HttpClient(new HttpClientHandler());
			httpClient.DefaultRequestHeaders.TryAddWithoutValidation("grant_type", "client_credentials");
			httpClient.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json") {CharSet = "UTF-8"});

			string data = null;
			if (!string.IsNullOrEmpty(serverRsaExponent) && !string.IsNullOrEmpty(serverRsaModulus) && !string.IsNullOrEmpty(challenge))
			{
				var exponent = OAuthHelper.ParseBytes(serverRsaExponent);
				var modulus = OAuthHelper.ParseBytes(serverRsaModulus);

				var apiKeyParts = ApiKey.Split(new[] {'/'}, StringSplitOptions.None);
				if (apiKeyParts.Length > 2)
				{
					apiKeyParts[1] = string.Join("/", apiKeyParts.Skip(1));
				}
				if (apiKeyParts.Length < 2)
					throw new InvalidOperationException("Invalid API key");

				var apiKeyName = apiKeyParts[0].Trim();
				var apiSecret = apiKeyParts[1].Trim();

				data = OAuthHelper.DictionaryToString(new Dictionary<string, string>
				{
					{OAuthHelper.Keys.RSAExponent, serverRsaExponent},
					{OAuthHelper.Keys.RSAModulus, serverRsaModulus},
					{
						OAuthHelper.Keys.EncryptedData,
						OAuthHelper.EncryptAsymmetric(exponent, modulus, OAuthHelper.DictionaryToString(new Dictionary<string, string>
						{
							{OAuthHelper.Keys.APIKeyName, apiKeyName},
							{OAuthHelper.Keys.Challenge, challenge},
							{OAuthHelper.Keys.Response, OAuthHelper.Hash(string.Format(OAuthHelper.Keys.ResponseFormat, challenge, apiSecret))}
						}))
					}
				});
			}

			var requestUri = oauthSource
#if SILVERLIGHT
				.NoCache()
#endif
				;
			var response = await httpClient.PostAsync(requestUri, data != null ? (HttpContent) new CompressedStringContent(data, true) : new StringContent(""))
			                               .AddUrlIfFaulting(new Uri(requestUri))
			                               .ConvertSecurityExceptionToServerNotFound();

			using (var stream = await response.GetResponseStreamWithHttpDecompression())
			using (var reader = new StreamReader(stream))
			{
				Dictionary<string, string> challengeDictionary;
				try
				{
					CurrentOauthToken = reader.ReadToEnd();
#if SILVERLIGHT
					BrowserCookieToAllowUserToUseStandardRequests(baseUrl, reader.ReadToEnd());
#endif
					return (Action<HttpClient>) (SetAuthorization);
				}
				catch (AggregateException ae)
				{
					var ex = ae.ExtractSingleInnerException() as WebException;
					if (tries > 2 || ex == null)
						// We've already tried three times and failed
						throw;

					var authResponse = ex.Response as HttpWebResponse;
					if (authResponse == null || authResponse.StatusCode != HttpStatusCode.PreconditionFailed)
						throw;

					var header = authResponse.Headers["Www-Authenticate"];
					if (string.IsNullOrEmpty(header) || !header.StartsWith(OAuthHelper.Keys.WWWAuthenticateHeaderKey))
						throw;

#if !NETFX_CORE
					authResponse.Close();
#endif

					challengeDictionary = OAuthHelper.ParseDictionary(header.Substring(OAuthHelper.Keys.WWWAuthenticateHeaderKey.Length).Trim());
				}

				return await DoOAuthRequestAsync(baseUrl, oauthSource,
						challengeDictionary.GetOrDefault(OAuthHelper.Keys.RSAExponent),
						challengeDictionary.GetOrDefault(OAuthHelper.Keys.RSAModulus),
						challengeDictionary.GetOrDefault(OAuthHelper.Keys.Challenge),
						tries + 1);
			}
		}

#if SILVERLIGHT
		private void BrowserCookieToAllowUserToUseStandardRequests(string baseUrl, string currentOauthToken)
		{
			var webRequest = WebRequestCreator.BrowserHttp.Create(new Uri(baseUrl + "/OAuth/Cookie"));
			webRequest.Headers["Authorization"] = "Bearer " + currentOauthToken;
			webRequest.Headers["Has-Api-Key"] = "True";
			webRequest.Method = "POST";
			webRequest.BeginGetResponse(ar =>
			{
				try
				{
					using (webRequest.EndGetResponse(ar))
					{

					}
				}
				catch (WebException we)
				{
					Debug.WriteLine("Failed to set browser cookie: " + we.Message);
					using(we.Response)
					{
						
					}
				}
				catch (Exception e)
				{
					Debug.WriteLine("Failed to set browser cookie: " + e.Message);
				}
			}, null);
		}
#endif
	}
}