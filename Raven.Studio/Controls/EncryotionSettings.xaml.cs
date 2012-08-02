using System;
using System.Security.Cryptography;
using System.Windows;
using System.Windows.Controls;
using Raven.Abstractions.Data;

namespace Raven.Studio.Controls
{
	public partial class EncryotionSettings : ChildWindow
	{
		private readonly RandomNumberGenerator RandomNumberGenerator;
		public EncryotionSettings()
		{
			RandomNumberGenerator = new RNGCryptoServiceProvider();
			InitializeComponent();
			EncryptionKey.Text = GenerateRandomKey();
		}

		private string GenerateRandomKey()
		{
			var result = new byte[Constants.DefaultGeneratedEncryptionKeyLength];
			RandomNumberGenerator.GetBytes(result);
			return Convert.ToBase64String(result); 
		}

		private void OKButton_Click(object sender, RoutedEventArgs e)
		{
			DialogResult = true;
		}

		private void CancelButton_Click(object sender, RoutedEventArgs e)
		{
			DialogResult = false;
		}
	}
}

