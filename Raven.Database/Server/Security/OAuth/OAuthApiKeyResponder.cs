using System;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Security.Cryptography;
using System.Text;
using Raven.Abstractions;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Extensions.Internal;
using Raven.Database.Extensions;
using Raven.Database.Server.Abstractions;
using Raven.Json.Linq;

namespace Raven.Database.Server.Security.OAuth
{
	public class OAuthApiKeyResponder : AbstractRequestResponder
	{

		private const int MaxOAuthContentLength = 1500;
		private static readonly TimeSpan MaxChallengeAge = TimeSpan.FromMinutes(10);

		static OAuthApiKeyResponder()
		{
			OAuthHelper.InitializeServerKeys();
		}

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
			var rsaExponent = requestContentsDictionary.TryGetValue(OAuthHelper.Keys.RSAExponent);
			var rsaModulus = requestContentsDictionary.TryGetValue(OAuthHelper.Keys.RSAModulus);
			if (!rsaExponent.SequenceEqual(OAuthHelper.RSAExponent) || !rsaModulus.SequenceEqual(OAuthHelper.RSAModulus))
			{
				RespondWithChallenge(context);
				return;
			}

			var challengeDictionary = OAuthHelper.ParseDictionary(OAuthHelper.DecryptAsymmetric(requestContentsDictionary.TryGetValue(OAuthHelper.Keys.EncryptedData)));
			var apiKeyName = challengeDictionary.TryGetValue(OAuthHelper.Keys.APIKeyName);
			var challenge = challengeDictionary.TryGetValue(OAuthHelper.Keys.Challenge);
			var response = challengeDictionary.TryGetValue(OAuthHelper.Keys.Response);

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

			var challengeData = OAuthHelper.ParseDictionary(OAuthHelper.DecryptSymmetric(challenge));
			var challengeTimestamp = OAuthHelper.ParseDateTime(challengeData.TryGetValue(OAuthHelper.Keys.ChallengeTimestamp));
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
				{ OAuthHelper.Keys.ChallengeTimestamp, OAuthHelper.DateTimeToString(SystemTime.UtcNow) },
				{ OAuthHelper.Keys.ChallengeSalt, OAuthHelper.BytesToString(OAuthHelper.RandomBytes(OAuthHelper.Keys.ChallengeSaltLength)) }
			};

			var responseData = new Dictionary<string, string>
			{
				{ OAuthHelper.Keys.RSAExponent, OAuthHelper.RSAExponent },
				{ OAuthHelper.Keys.RSAModulus, OAuthHelper.RSAModulus },
				{ OAuthHelper.Keys.Challenge, OAuthHelper.EncryptSymmetric(OAuthHelper.DictionaryToString(challengeData)) }
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
