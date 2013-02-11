// -----------------------------------------------------------------------
//  <copyright file="MD5.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Windows.Security.Cryptography;
using Windows.Security.Cryptography.Core;
using Windows.Storage.Streams;

namespace Raven.Client.WinRT.MissingFromWinRT
{
	public class MD5
	{
		public static byte[] HashCore(byte[] bytes)
		{
			var alg = HashAlgorithmProvider.OpenAlgorithm("MD5");
			var buff = CryptographicBuffer.CreateFromByteArray(bytes);
			var hashed = alg.HashData(buff);
			byte[] result;
			CryptographicBuffer.CopyToByteArray(hashed, out result);
			return result;
		}
	}
}