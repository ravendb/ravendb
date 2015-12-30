// -----------------------------------------------------------------------
//  <copyright file="FileSystemSmugglerOperationState.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
namespace Raven.Abstractions.Database.Smuggler.FileSystem
{
    public class FileSystemSmugglerOperationState : LastFilesEtagsInfo
    {
        public string OutputPath { get; set; }
    }
}