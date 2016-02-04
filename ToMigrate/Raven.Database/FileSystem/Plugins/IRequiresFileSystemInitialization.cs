// -----------------------------------------------------------------------
//  <copyright file="IRequiresFileSystemInitialization.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
namespace Raven.Database.FileSystem.Plugins
{
    public interface IRequiresFileSystemInitialization
    {
        void Initialize(RavenFileSystem fileSystem);

        void SecondStageInit(); 
    }
}
