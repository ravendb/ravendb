// -----------------------------------------------------------------------
//  <copyright file="SmugglerException.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
namespace Raven.Abstractions.Exceptions
{
    using System;

    public class SmugglerException : Exception
    {
        public SmugglerException()
        {
        }

        public SmugglerException(string message)
            : base(message)
        {
        }

        public SmugglerException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }
}