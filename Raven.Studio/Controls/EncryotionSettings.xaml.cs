using System;
using System.Security.Cryptography;
using System.Windows;
using System.Windows.Controls;

namespace Raven.Studio.Controls
{
	public partial class EncryotionSettings : ChildWindow
	{
		private RandomNumberGenerator RandomNumberGenerator;
		public EncryotionSettings()
		{
			RandomNumberGenerator = new RNGCryptoServiceProvider();
			InitializeComponent();
			EncryptionKey.Text = GenerateRandomKey();
		}

		private string GenerateRandomKey()
		{
			int length = 32;
			var result = new byte[length];
			RandomNumberGenerator.GetBytes(result);
			return Convert.ToBase64String(result); 
		}

		private void OKButton_Click(object sender, RoutedEventArgs e)
		{
			this.DialogResult = true;
		}

		private void CancelButton_Click(object sender, RoutedEventArgs e)
		{
			this.DialogResult = false;
		}
	}
}

