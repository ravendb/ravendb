// -----------------------------------------------------------------------
//  <copyright file="SettingsDat.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.IO;
using System.Security.Cryptography;
using Xunit;

namespace Raven.Tests.Utils
{
	public class SettingsDat
	{
		// [Fact]
		public void GenerateSettingsDatFile()
		{
			using (var file = File.Create(@"C:\work\RavenDB\Raven.Studio\Settings.dat"))
			{
				using (var aes = new AesManaged())
				{
					file.Write(aes.Key, 0, aes.Key.Length);
					file.Write(aes.IV, 0, aes.IV.Length);
					using (var cryptoStream = new CryptoStream(file, aes.CreateEncryptor(), CryptoStreamMode.Write))
					using (var cryptoReader = new BinaryWriter(cryptoStream))
					{
						cryptoReader.Write("Username");
						cryptoReader.Write("Password");
					}
				}
			}
		}
	}
}