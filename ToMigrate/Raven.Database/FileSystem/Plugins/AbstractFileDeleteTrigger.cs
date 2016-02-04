// -----------------------------------------------------------------------
//  <copyright file="AbstractFileDeleteTrigger.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.ComponentModel.Composition;

using Raven.Database.Plugins;

namespace Raven.Database.FileSystem.Plugins
{
    [InheritedExport]
    public abstract class AbstractFileDeleteTrigger : IRequiresFileSystemInitialization
    {
        public RavenFileSystem FileSystem { get; private set; }

        public void Initialize(RavenFileSystem fileSystem)
        {
            FileSystem = fileSystem;
            Initialize();
        }

        public virtual void SecondStageInit()
        {
        }

        public virtual void Initialize()
        {

        }

        public virtual VetoResult AllowDelete(string name)
        {
            return VetoResult.Allowed;
        }

        public virtual void AfterDelete(string name)
        {
        }
    }
}
