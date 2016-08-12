// -----------------------------------------------------------------------
//  <copyright file="SmugglerException.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Runtime.Serialization;

namespace Raven.Abstractions.Exceptions
{
    using System;

    [Serializable]
    public class SmugglerException : Exception
    {
        //
        // For guidelines regarding the creation of new exception types, see
        //    http://msdn.microsoft.com/library/default.asp?url=/library/en-us/cpgenref/html/cpconerrorraisinghandlingguidelines.asp
        // and
        //    http://msdn.microsoft.com/library/default.asp?url=/library/en-us/dncscol/html/csharp07192001.asp
        //

        public SmugglerException()
        {
        }

        public SmugglerException(string message) : base(message)
        {
        }

        public SmugglerException(string message, Exception inner) : base(message, inner)
        {
        }

        protected SmugglerException(
            SerializationInfo info,
            StreamingContext context) : base(info, context)
        {
        }
    }
}
