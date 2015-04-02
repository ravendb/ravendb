using System;
using System.IO;
using System.Security.Cryptography;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Bundles.Encryption.Settings;
using Raven.Bundles.Encryption.Streams;
using Raven.Json.Linq;
using Raven.Tests.Bundles.Versioning;
using Xunit;
using Xunit.Extensions;

namespace Raven.Tests.Bundles.Encryption
{
	public class Crud : Encryption
	{
		[Theory]
		[InlineData(256000)]
		[InlineData(14300)]
		[InlineData(256)]
		[InlineData(512)]
		[InlineData(1024 * 2)]
		[InlineData(1024 * 4)]
		[InlineData(1024 * 8)]
		[InlineData(1024 * 16)]
		public void CryptoStream_should_read_and_write_parts_of_the_stream_properly(int expectedSizeInBytes)
		{
			var data = new byte[expectedSizeInBytes];
			new Random().NextBytes(data);

			const string encryptionKey = "Byax1jveejqio9Urcdjw8431iQYKkPg6Ig4OxHdxSAU=";
			var encryptionKeyBytes = Convert.FromBase64String(encryptionKey);
			var encryptionSettings = new EncryptionSettings(encryptionKeyBytes, typeof(RijndaelManaged), true, 128);

			var filename = Guid.NewGuid() + ".txt";
			try
			{

				using (var stream = new FileStream(filename, FileMode.CreateNew))
				using (var cryptoStream = new SeekableCryptoStream(encryptionSettings, encryptionKey, stream))
				{
					cryptoStream.Write(data, 0, data.Length);
					cryptoStream.Flush();
				}

				using (var stream = new FileStream(filename, FileMode.Open))
				using (var cryptoStream = new SeekableCryptoStream(encryptionSettings, encryptionKey, stream))
				{
					var readData = new byte[data.Length];
					int position = 0;
					int bytesRead;
					do
					{
						bytesRead = cryptoStream.Read(readData, position, Math.Min(256,data.Length - position));
						position += bytesRead;
					} while (bytesRead > 0);
					Assert.Equal(data, readData);
				}
			}
			finally
			{
				File.Delete(filename);
			}
		}

		[Theory]
		[InlineData(256000)]
		[InlineData(14300)]
		[InlineData(256)]
		[InlineData(512)]
		[InlineData(1024 * 2)]
		[InlineData(1024 * 4)]
		[InlineData(1024 * 8)]
		[InlineData(1024 * 16)]
		public void CryptoStream_should_read_and_write_properly(int expectedSizeInBytes)
		{
			var data = new byte[expectedSizeInBytes];
			new Random().NextBytes(data);

			const string encryptionKey = "Byax1jveejqio9Urcdjw8431iQYKkPg6Ig4OxHdxSAU=";
			var encryptionKeyBytes = Convert.FromBase64String(encryptionKey);
			var encryptionSettings = new EncryptionSettings(encryptionKeyBytes, typeof(RijndaelManaged), true, 128);

			var filename = Guid.NewGuid() + ".txt";
			try
			{

				using (var stream = new FileStream(filename, FileMode.CreateNew))
				using (var cryptoStream = new SeekableCryptoStream(encryptionSettings, encryptionKey, stream))
				{
					cryptoStream.Write(data, 0, data.Length);
					cryptoStream.Flush();
				}

				using (var stream = new FileStream(filename, FileMode.Open))
				using (var cryptoStream = new SeekableCryptoStream(encryptionSettings, encryptionKey, stream))
				{
					var readData = cryptoStream.ReadData();
					Assert.Equal(data, readData);
				}
			}
			finally
			{
				File.Delete(filename);
			}
		}


		[Theory]
		[InlineData(256000)]
		[InlineData(14300)]
		[InlineData(256)]
		[InlineData(512)]
		[InlineData(1024 * 2)]
		[InlineData(1024 * 4)]
		[InlineData(1024 * 8)]
		[InlineData(1024 * 16)]
		public void CryptoStream_should_show_unencrypted_length_properly(int expectedSizeInBytes)
		{
			var data = new byte[expectedSizeInBytes];
			new Random().NextBytes(data);

			const string encryptionKey = "Byax1jveejqio9Urcdjw8431iQYKkPg6Ig4OxHdxSAU=";
			var encryptionKeyBytes = Convert.FromBase64String(encryptionKey);
			var encryptionSettings = new EncryptionSettings(encryptionKeyBytes, typeof(RijndaelManaged), true, 128);

			var filename = Guid.NewGuid() + ".txt";
			try
			{

				using (var stream = new FileStream(filename, FileMode.CreateNew))
				using (var cryptoStream = new SeekableCryptoStream(encryptionSettings, encryptionKey, stream))
				{
					cryptoStream.Write(data, 0, data.Length);
					cryptoStream.Flush();
				}

				using (var stream = new FileStream(filename, FileMode.Open))
				using (var cryptoStream = new SeekableCryptoStream(encryptionSettings, encryptionKey, stream))
				{
					Assert.Equal(data.Length, cryptoStream.Length);
				}
			}
			finally
			{
				File.Delete(filename);
			}
		}

		[Fact]
		public void StoreAndLoad()
		{
			const string CompanyName = "Company Name";
			var company = new Company { Name = CompanyName };
			using (var session = documentStore.OpenSession())
			{
				session.Store(company);
				session.SaveChanges();
			}

			using (var session = documentStore.OpenSession())
			{
				Assert.Equal(company.Name, session.Load<Company>(1).Name);
			}

			AssertPlainTextIsNotSavedInDatabase(CompanyName);
		}

		[Fact]
		public void Transactional()
		{
			const string FirstCompany = "FirstCompany";

			// write in transaction
			documentStore.DatabaseCommands.Put("docs/1", null,
											   new RavenJObject
			                                   	{
			                                   		{"Name", FirstCompany}
			                                   	},
											   new RavenJObject
			                                   	{
			                                   		{
			                                   			"Raven-Transaction-Information", Guid.NewGuid() + ", " + TimeSpan.FromMinutes(1)
			                                   		}
			                                   	});

			var jsonDocument = documentStore.DatabaseCommands.Get("docs/1");
			Assert.True(jsonDocument.Metadata.Value<bool>(Constants.RavenDocumentDoesNotExists));


			AssertPlainTextIsNotSavedInDatabase(FirstCompany);
		}
	}
}