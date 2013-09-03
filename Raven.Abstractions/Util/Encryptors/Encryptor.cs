// -----------------------------------------------------------------------
//  <copyright file="Encryptor.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
namespace Raven.Abstractions.Util.Encryptors
{
	using System;

	public static class Encryptor
	{
		private static IEncryptor current = new DefaultEncryptor();

		public static IEncryptor Current
		{
			get
			{
				return current;
			}
		}

		public static Lazy<bool> IsFipsEnabled
		{
			get
			{
				return new Lazy<bool>(() =>
				{
					try
					{
						current = new DefaultEncryptor();
						current.Hash.Compute16(new byte[] { 1 });

						return false;
					}
					catch (Exception)
					{
						return true;
					}
				});
			}
		}

		public static void Initialize(bool useFips)
		{
			current = useFips ? (IEncryptor)new FipsEncryptor() : new DefaultEncryptor();
		}

		public static void Dispose()
		{
			current = null;
		}
	}
}