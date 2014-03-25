using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using System.Web.Http;
using Raven.Abstractions;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Database.Server.Security.OAuth;

namespace Raven.Database.Server.Controllers
{
	public class OAuthController : RavenDbApiController
	{
		const string TokenContentType = "application/json; charset=UTF-8";
		const string TokenGrantType = "client_credentials";
		private const int MaxOAuthContentLength = 1500;
		private static readonly TimeSpan MaxChallengeAge = TimeSpan.FromMinutes(10);

		private int numberOfTokensIssued;
		public int NumberOfTokensIssued
		{
			get { return numberOfTokensIssued; }
		}

		[HttpPost][Route("OAuth/API-Key")]
		public async Task<HttpResponseMessage> ApiKeyPost()
		{
			if (InnerRequest.Content.Headers.ContentLength > MaxOAuthContentLength)
			{
				return
					GetMessageWithObject(
						new
						{
							error = "invalid_request",
							error_description = "Content length should not be over " + MaxOAuthContentLength + " bytes"
						},
						HttpStatusCode.BadRequest);
			}

			if (InnerRequest.Content.Headers.ContentLength == 0)
				return RespondWithChallenge();

			//using (var reader = new StreamReader(context.Request.InputStream))
			//	requestContents = reader.ReadToEnd();
			var requestContents = await ReadStringAsync();

			var requestContentsDictionary = OAuthHelper.ParseDictionary(requestContents);
			var rsaExponent = requestContentsDictionary.GetOrDefault(OAuthHelper.Keys.RSAExponent);
			var rsaModulus = requestContentsDictionary.GetOrDefault(OAuthHelper.Keys.RSAModulus);
			if (rsaExponent == null || rsaModulus == null ||
				!rsaExponent.SequenceEqual(OAuthServerHelper.RSAExponent) || !rsaModulus.SequenceEqual(OAuthServerHelper.RSAModulus))
			{
				return RespondWithChallenge();
			}

			var encryptedData = requestContentsDictionary.GetOrDefault(OAuthHelper.Keys.EncryptedData);
			if (string.IsNullOrEmpty(encryptedData))
				return RespondWithChallenge();

			var challengeDictionary = OAuthHelper.ParseDictionary(OAuthServerHelper.DecryptAsymmetric(encryptedData));
			var apiKeyName = challengeDictionary.GetOrDefault(OAuthHelper.Keys.APIKeyName);
			var challenge = challengeDictionary.GetOrDefault(OAuthHelper.Keys.Challenge);
			var response = challengeDictionary.GetOrDefault(OAuthHelper.Keys.Response);

			if (string.IsNullOrEmpty(apiKeyName) || string.IsNullOrEmpty(challenge) || string.IsNullOrEmpty(response))
				return RespondWithChallenge();

			var challengeData = OAuthHelper.ParseDictionary(OAuthServerHelper.DecryptSymmetric(challenge));
			var timestampStr = challengeData.GetOrDefault(OAuthHelper.Keys.ChallengeTimestamp);
			if (string.IsNullOrEmpty(timestampStr))
				return RespondWithChallenge();

			var challengeTimestamp = OAuthServerHelper.ParseDateTime(timestampStr);
			if (challengeTimestamp + MaxChallengeAge < SystemTime.UtcNow || challengeTimestamp > SystemTime.UtcNow)
			{
				// The challenge is either old or from the future 
				return RespondWithChallenge();
			}

			var apiKeyTuple = GetApiKeySecret(apiKeyName);
			if (apiKeyTuple == null)
			{
				return GetMessageWithObject(new {error = "unauthorized_client", error_description = "Unknown API Key"},
					HttpStatusCode.Unauthorized);
			}

			var apiSecret = apiKeyTuple.Item1;
			if (string.IsNullOrEmpty(apiKeyName))
			{
				return GetMessageWithObject(new {error = "unauthorized_client", error_description = "Invalid API Key"},
					HttpStatusCode.Unauthorized);
			}

			var expectedResponse = OAuthHelper.Hash(string.Format(OAuthHelper.Keys.ResponseFormat, challenge, apiSecret));
			if (response != expectedResponse)
			{
				return GetMessageWithObject(new {error = "unauthorized_client", error_description = "Invalid challenge response"},
					HttpStatusCode.Unauthorized);
			}

			var token = apiKeyTuple.Item2;

			return GetMessageWithObject(token);
		}

		public HttpResponseMessage RespondWithChallenge()
		{
			var challengeData = new Dictionary<string, string>
			{
				{ OAuthHelper.Keys.ChallengeTimestamp, OAuthServerHelper.DateTimeToString(SystemTime.UtcNow) },
				{ OAuthHelper.Keys.ChallengeSalt, OAuthHelper.BytesToString(OAuthServerHelper.RandomBytes(OAuthHelper.Keys.ChallengeSaltLength)) }
			};

			var responseData = new Dictionary<string, string>
			{
				{ OAuthHelper.Keys.RSAExponent, OAuthServerHelper.RSAExponent },
				{ OAuthHelper.Keys.RSAModulus, OAuthServerHelper.RSAModulus },
				{ OAuthHelper.Keys.Challenge, OAuthServerHelper.EncryptSymmetric(OAuthHelper.DictionaryToString(challengeData)) }
			};

			var msg = GetEmptyMessage(HttpStatusCode.PreconditionFailed);
			var value = OAuthHelper.Keys.WWWAuthenticateHeaderKey + " " + OAuthHelper.DictionaryToString(responseData);
			msg.Headers.TryAddWithoutValidation("WWW-Authenticate", value);

			return msg;
		}

		private Tuple<string, AccessToken> GetApiKeySecret(string apiKeyName)
		{
			var document = DatabasesLandlord.SystemDatabase.Documents.Get("Raven/ApiKeys/" + apiKeyName, null);
			if (document == null)
				return null;

			var apiKeyDefinition = document.DataAsJson.JsonDeserialization<ApiKeyDefinition>();
			if (apiKeyDefinition.Enabled == false)
				return null;

			return Tuple.Create(apiKeyDefinition.Secret, AccessToken.Create(DatabasesLandlord.SystemConfiguration.OAuthTokenKey, new AccessTokenBody
			{
				UserId = apiKeyName,
				AuthorizedDatabases = apiKeyDefinition.Databases,
                AuthorizedFileSystems = apiKeyDefinition.FileSystems
			}));
		}
	}
}