// -----------------------------------------------------------------------
//  <copyright file="DocumentsContextPool.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Server.Files;
using Sparrow.Json;

namespace Raven.Server.ServerWide.Context
{
    public class FilesContextPool : JsonContextPoolBase<FilesOperationContext>
    {
        private readonly FileSystem _fileSystem;

        public FilesContextPool(FileSystem fileSystem)
        {
            _fileSystem = fileSystem;
        }

        protected override FilesOperationContext CreateContext()
        {
            return new FilesOperationContext(_fileSystem, 1024*1024, 16*1024);
        }
    }
}