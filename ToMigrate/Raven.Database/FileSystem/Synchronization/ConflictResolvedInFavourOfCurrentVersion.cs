// -----------------------------------------------------------------------
//  <copyright file="ConflictResolvedInFavourOfCurrentVersion.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Runtime.Serialization;

namespace Raven.Database.FileSystem.Synchronization
{
    [Serializable]
    public class ConflictResolvedInFavourOfCurrentVersionException : Exception
    {
        public ConflictResolvedInFavourOfCurrentVersionException()
        {
        }

        public ConflictResolvedInFavourOfCurrentVersionException(string message) : base(message)
        {
        }

        public ConflictResolvedInFavourOfCurrentVersionException(string message, Exception inner) : base(message, inner)
        {
        }

        protected ConflictResolvedInFavourOfCurrentVersionException(
            SerializationInfo info,
            StreamingContext context) : base(info, context)
        {
        }
    }
}
