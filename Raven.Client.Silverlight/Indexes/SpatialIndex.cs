//-----------------------------------------------------------------------
// <copyright file="SpatialIndex.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;

namespace Raven.Client.Indexes
{
      /// <summary>
    /// Accessor for generating spatial queries on the client side
    /// </summary>
    public static class SpatialIndex
    {
        /// <summary>
        /// Marker method for allowing generating spatial indexes on the client
        /// </summary>
        public static object Generate(double lat, double lng)
        {
            throw new NotSupportedException("This method is provided solely to allow query translation on the server");
        }

    }
}