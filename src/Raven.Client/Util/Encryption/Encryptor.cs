// -----------------------------------------------------------------------
//  <copyright file="Encryptor.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;

namespace Raven.Client.Util.Encryption
{
    internal static class Encryptor
    {
        static Encryptor()
        {
            Current = new DefaultEncryptor();
        }

        public static IEncryptor Current { get; private set; }
    }
}
