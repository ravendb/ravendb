// -----------------------------------------------------------------------
//  <copyright file="HideSyncingFileTrigger.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.ComponentModel.Composition;

using Raven.Database.FileSystem.Util;
using Raven.Database.Plugins;
using Raven.Json.Linq;

namespace Raven.Database.FileSystem.Plugins.Builtins
{
    [InheritedExport(typeof(AbstractFileReadTrigger))]
    public class HideDownloadingFileTrigger : AbstractFileReadTrigger
    {
        public override ReadVetoResult AllowRead(string name, RavenJObject metadata, ReadOperation operation)
        {
            if (name.EndsWith(RavenFileNameHelper.DownloadingFileSuffix))
                return ReadVetoResult.Ignore;

            return ReadVetoResult.Allowed;
        }
    }
}