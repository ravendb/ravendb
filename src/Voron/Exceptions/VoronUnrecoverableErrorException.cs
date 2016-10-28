// -----------------------------------------------------------------------
//  <copyright file="VoronUnrecoverableErrorException.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Runtime.ExceptionServices;

namespace Voron.Exceptions
{
    public class VoronUnrecoverableErrorException : Exception
    {
        public static void Raise(StorageEnvironment env, string message)
        {
            try
            {
                throw new VoronUnrecoverableErrorException(message);
            }
            catch (Exception e)
            {
                if (env != null)
                    env.CatastrophicFailure = ExceptionDispatchInfo.Capture(e);
                throw;
            }
        }

        public static void Raise(StorageEnvironment env, string message, Exception inner)
        {
            try
            {
                throw new VoronUnrecoverableErrorException(message, inner);
            }
            catch (Exception e)
            {
                env.CatastrophicFailure = ExceptionDispatchInfo.Capture(e);
                throw;
            }
        }

        private VoronUnrecoverableErrorException(string message)
            : base(message)
        {
        }

        private VoronUnrecoverableErrorException(string message, Exception inner)
            : base(message, inner)
        {
        }

    }
}
