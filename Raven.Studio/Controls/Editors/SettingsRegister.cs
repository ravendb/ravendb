// -----------------------------------------------------------------------
//  <copyright file="SettingsRegister.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Diagnostics;
using System.IO;
using System.Security.Cryptography;
using System.Windows;

namespace Raven.Studio.Controls.Editors
{
	public class SettingsRegister
	{
		public static void Register()
		{
			using (var manifestResourceStream = typeof(SettingsRegister).Assembly.GetManifestResourceStream("Raven.Studio.Settings.dat"))
			{
				if (manifestResourceStream == null || manifestResourceStream.Length == 0)
				{
					return;
				}

				using (var reader = new BinaryReader(manifestResourceStream))
				using (var aes = new AesManaged())
				{
					aes.Key = reader.ReadBytes(32);
					aes.IV = reader.ReadBytes(16);

					using (var cryptoStream = new CryptoStream(manifestResourceStream, aes.CreateDecryptor(), CryptoStreamMode.Read))
					using (var cryptoReader = new BinaryReader(cryptoStream))
					{
						var licensee = cryptoReader.ReadString();
						var licenseKey = cryptoReader.ReadString();
						ActiproSoftware.Products.ActiproLicenseManager.RegisterLicense(licensee, licenseKey);
					}
				}
			}
		} 
	}
}