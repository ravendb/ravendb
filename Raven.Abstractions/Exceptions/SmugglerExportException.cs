// -----------------------------------------------------------------------
//  <copyright file="SmugglerExportException.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using Raven.Abstractions.Data;

namespace Raven.Abstractions.Exceptions
{
    public class SmugglerExportException : Exception
    {
        public SmugglerExportException()
        {
        }

        public SmugglerExportException(string message) : base(message)
        {
        }

        public SmugglerExportException(string message, Exception innerException) : base(message, innerException)
        {
        }

        public Etag LastEtag { get; set; }

        public string File { get; set; }


    }

}