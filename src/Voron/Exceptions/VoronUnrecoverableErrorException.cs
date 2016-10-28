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
        public VoronUnrecoverableErrorException(StorageEnvironment env, string message) 
            : base(message)
        {
            env.CatastrophicFailure = ExceptionDispatchInfo.Capture(this);
        }

        public VoronUnrecoverableErrorException(StorageEnvironment env, string message, Exception inner) 
            : base(message, inner)
        {
            env.CatastrophicFailure = ExceptionDispatchInfo.Capture(this);
        }

        public VoronUnrecoverableErrorException(string message, Exception inner)
            : base(message, inner)
        {
        }

    }
}
