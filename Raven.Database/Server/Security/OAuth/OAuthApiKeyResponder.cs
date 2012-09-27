using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Raven.Abstractions;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Extensions;
using Raven.Database.Extensions;
using Raven.Database.Server.Abstractions;

namespace Raven.Database.Server.Security.OAuth
{
	public class OAuthApiKeyResponder : AbstractRequestResponder
	{

		private const int MaxOAuthContentLength = 1500;
		private static readonly TimeSpan MaxChallengeAge = TimeSpan.FromMinutes(10);

		public override string UrlPattern
		{
			get { return @"^/OAuth/API-Key$"; }
		}

		public override string[] SupportedVerbs
		{
			get { return new[] { "POST" }; }
		}

		public override void Respond(IHttpContext context)
		{
			if (context.Request.ContentLength > MaxOAuthContentLength)
			{
				context.SetStatusToBadRequest();
				context.WriteJson(new { error = "invalid_request", error_description = "Content length should not be over " + MaxOAuthContentLength + " bytes" });
				return;
			}

			if (context.Request.ContentLength == 0)
			{
				RespondWithChallenge(context);
				return;
			}

			string requestContents;
			using (var reader = new StreamReader(context.Request.InputStream))
				requestContents = reader.ReadToEnd();

			var requestContentsDictionary = OAuthHelper.ParseDictionary(requestContents);
			var rsaExponent = requestContentsDictionary.GetOrDefault(OAuthHelper.Keys.RSAExponent);
			var rsaModulus = requestContentsDictionary.GetOrDefault(OAuthHelper.Keys.RSAModulus);
			if (rsaExponent == null || rsaModulus == null || 
				!rsaExponent.SequenceEqual(OAuthServerHelper.RSAExponent) || !rsaModulus.SequenceEqual(OAuthServerHelper.RSAModulus))
			{
				RespondWithChallenge(context);
				return;
			}

			var encryptedData = requestContentsDictionary.GetOrDefault(OAuthHelper.Keys.EncryptedData);
			if(string.IsNullOrEmpty(encryptedData))
			{
				RespondWithChallenge(context);
				return;
			}

			var challengeDictionary = OAuthHelper.ParseDictionary(OAuthServerHelper.DecryptAsymmetric(encryptedData));
			var apiKeyName = challengeDictionary.GetOrDefault(OAuthHelper.Keys.APIKeyName);
			var challenge = challengeDictionary.GetOrDefault(OAuthHelper.Keys.Challenge);
			var response = challengeDictionary.GetOrDefault(OAuthHelper.Keys.Response);

			if (string.IsNullOrEmpty(apiKeyName) || string.IsNullOrEmpty(challenge) || string.IsNullOrEmpty(response))
			{
				RespondWithChallenge(context);
				return;
			}

			var apiSecret = GetApiKeySecret(apiKeyName);
			if (string.IsNullOrEmpty(apiSecret))
			{
				context.SetStatusToUnauthorized();
				context.WriteJson(new { error = "unauthorized_client", error_description = "Unknown API Key" });
				return;
			}

			var challengeData = OAuthHelper.ParseDictionary(OAuthServerHelper.DecryptSymmetric(challenge));
			var timestampStr = challengeData.GetOrDefault(OAuthHelper.Keys.ChallengeTimestamp);
			if(string.IsNullOrEmpty(timestampStr))
			{
				RespondWithChallenge(context);
				return;
			}
			var challengeTimestamp = OAuthServerHelper.ParseDateTime(timestampStr);
			if (challengeTimestamp + MaxChallengeAge < SystemTime.UtcNow || challengeTimestamp > SystemTime.UtcNow)
			{
				// The challenge is either old or from the future 
				RespondWithChallenge(context);
				return;
			}

			var expectedResponse = OAuthHelper.Hash(string.Format(OAuthHelper.Keys.ResponseFormat, challenge, apiSecret));
			if (response != expectedResponse)
			{
				context.SetStatusToUnauthorized();
				context.WriteJson(new { error = "unauthorized_client", error_description = "Invalid challenge response" });
				return;
			}

			var token = GetAccessTokenFromApiKey(apiKeyName, apiSecret);
			if (token == null)
			{
				context.SetStatusToUnauthorized();
				context.WriteJson(new { error = "unauthorized_client", error_description = "Invalid client credentials" });

				return;
			}

			context.Write(token.Serialize());
		}

		public void RespondWithChallenge(IHttpContext context)
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

			context.SetStatusToUnauthorized();
			context.Response.AddHeader("WWW-Authenticate", OAuthHelper.Keys.WWWAuthenticateHeaderKey + " " + OAuthHelper.DictionaryToString(responseData));
		}

		private AccessToken GetAccessTokenFromApiKey(string apiKeyName, string apiSecret)
		{
			return AccessToken.Create(Settings.OAuthTokenCertificate, new AccessTokenBody
			{
				UserId = apiKeyName,
				AuthorizedDatabases = new AccessTokenBody.DatabaseAccess[] { }
			});
		}

		private string GetApiKeySecret(string apiKeyName)
		{
			return "ThisIsMySecret";
		}
	}
}
