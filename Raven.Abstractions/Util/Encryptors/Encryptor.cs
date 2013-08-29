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
		private static IEncryptor current;

		public static IEncryptor Current
		{
			get
			{
				if (current == null)
					throw new InvalidOperationException("Did you forget to initialize encryptor?");

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
						current.Hash.Compute(new byte[] { 1 });

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