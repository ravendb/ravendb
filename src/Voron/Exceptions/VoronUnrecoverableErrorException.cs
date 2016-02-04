// -----------------------------------------------------------------------
//  <copyright file="VoronUnrecoverableErrorException.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;

namespace Voron.Exceptions
{
    public class VoronUnrecoverableErrorException : Exception
    {
         public VoronUnrecoverableErrorException()
        {
        }

        public VoronUnrecoverableErrorException(string message) 
            : base(message)
        {
        }

        public VoronUnrecoverableErrorException(string message, Exception inner) 
            : base(message, inner)
        {
        }

    }
}
