// -----------------------------------------------------------------------
//  <copyright file="SmugglerExportException.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Runtime.Serialization;
using Raven.Abstractions.Data;

namespace Raven.Abstractions.Exceptions
{

    //
    // For guidelines regarding the creation of new exception types, see
    //    http://msdn.microsoft.com/library/default.asp?url=/library/en-us/cpgenref/html/cpconerrorraisinghandlingguidelines.asp
    // and
    //    http://msdn.microsoft.com/library/default.asp?url=/library/en-us/dncscol/html/csharp07192001.asp
    //

    [Serializable]
    public class SmugglerExportException : System.Exception
    {
        public SmugglerExportException()
        {
        }

        public SmugglerExportException(string message) : base(message)
        {
        }

        public SmugglerExportException(string message, System.Exception inner) : base(message, inner)
        {
        }


        public Etag LastEtag { get; set; }

        public string File { get; set; }

        protected SmugglerExportException(SerializationInfo info, StreamingContext context) : base(info, context)
        {
        }
    }

    [Serializable]
    public class SmugglerImportException : Exception
    {
        public SmugglerImportException()
        {
        }

        public SmugglerImportException(string message) : base(message)
        {
        }

        public SmugglerImportException(string message, System.Exception inner) : base(message, inner)
        {
        }

        public Etag LastEtag { get; set; }

        protected SmugglerImportException(SerializationInfo info, StreamingContext context) : base(info, context)
        {
        }
    }
}
