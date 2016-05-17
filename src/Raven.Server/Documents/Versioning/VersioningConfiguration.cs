//-----------------------------------------------------------------------
// <copyright file="VersioningConfiguration.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System.Collections.Generic;

namespace Raven.Server.Documents.Versioning
{
    public class VersioningConfiguration
    {
        public VersioningConfigurationCollection Default { get; set; }

        public Dictionary<string, VersioningConfigurationCollection> Collections { get; set; }
    }
}