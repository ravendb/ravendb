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
		static Encryptor()
		{
			Current = new DefaultEncryptor();
		}

		public static IEncryptor Current { get; private set; }

		public static Lazy<bool> IsFipsEnabled
		{
			get
			{
				return new Lazy<bool>(() =>
				{
					try
					{
						var defaultEncryptor = new DefaultEncryptor();
						defaultEncryptor.Hash.Compute16(new byte[] { 1 });

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
			Current = useFips ? (IEncryptor)new FipsEncryptor() : new DefaultEncryptor();
		}
	}
}