using System;
using System.IO;
using System.IO.Compression;
using System.Net;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using Raven.Bundles.Authentication;
using Raven.Http.Security.OAuth;
using Xunit;

namespace Raven.Bundles.Tests.Authentication
{
	public class SimpleLogin : AuthenticationTest
	{
		[Fact]
		public void CanLogin()
		{
			using(var session = store.OpenSession())
			{
				session.Store(new AuthenticationUser
				{
					Name = "Ayende",
					Id = "Raven/Users/Ayende",
					AllowedDatabases = new[] {"*"}
				}.SetPassword("abc"));
				session.SaveChanges();
			}

			var req = (HttpWebRequest) WebRequest.Create(server.Database.Configuration.ServerUrl + "/OAuth/AccessToken");
			var response = req.WithBasicCredentials("Ayende", "abc")
				.WithConentType("application/json;charset=UTF-8")
				.WithHeader("grant_type", "client_credentials")
				.MakeRequest()
				.ReadToEnd();

			AccessTokenBody body;
			Assert.True(AccessToken.TryParseBody(new X509Certificate2(GetPath(@"Authentication\Public.cer")), response, out body));
		}

		public static string CompressString(string text)
		{
			byte[] buffer = Encoding.UTF8.GetBytes(text);
			var memoryStream = new MemoryStream();
			using (var gZipStream = new DeflateStream(memoryStream, CompressionMode.Compress, true))
			{
				gZipStream.Write(buffer, 0, buffer.Length);
			}

			memoryStream.Position = 0;

			var compressedData = new byte[memoryStream.Length];
			memoryStream.Read(compressedData, 0, compressedData.Length);

			var gZipBuffer = new byte[compressedData.Length + 4];
			Buffer.BlockCopy(compressedData, 0, gZipBuffer, 4, compressedData.Length);
			Buffer.BlockCopy(BitConverter.GetBytes(buffer.Length), 0, gZipBuffer, 0, 4);
			return Convert.ToBase64String(gZipBuffer);
		}
	}
}