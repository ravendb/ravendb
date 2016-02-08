// -----------------------------------------------------------------------
//  <copyright file="SmugglerException.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Runtime.Serialization;

using Raven.Abstractions.Data;

namespace Raven.Abstractions.Exceptions
{
    public class SmugglerException : Exception
    {
        public SmugglerException()
        {
        }

        public SmugglerException(string message) : base(message)
        {
        }

        public SmugglerException(string message, Exception inner) : base(message, inner)
        {
        }

        public long? LastEtag { get; set; }

        public string File { get; set; }
    }
}
