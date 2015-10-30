// -----------------------------------------------------------------------
//  <copyright file="AbstractFileCodec.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.ComponentModel.Composition;
using System.IO;
using Raven.Json.Linq;

namespace Raven.Database.FileSystem.Plugins
{
    [InheritedExport]
    public abstract class AbstractFileCodec : IRequiresFileSystemInitialization
    {
        public RavenFileSystem FileSystem { get; set; }

        public virtual void Initialize(RavenFileSystem fileSystem)
        {
            FileSystem = fileSystem;
            Initialize();
        }

        public virtual void Initialize()
        {

        }

        public virtual void SecondStageInit()
        {

        }

        public abstract Stream EncodePage(Stream data);

        public abstract Stream DecodePage(Stream encodedDataStream);
    }
}
