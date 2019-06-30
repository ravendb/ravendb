using System;
using System.Collections.Generic;
using System.Text;
using Raven.Client.Exceptions;

namespace Raven.Server.Documents.Patch
{
    public class PatchFailedException : RavenException
    {
        public PatchFailedException(string id, Exception exception) : base($"Failed to patch document: {id}",exception)
        {
        }
    }
}
