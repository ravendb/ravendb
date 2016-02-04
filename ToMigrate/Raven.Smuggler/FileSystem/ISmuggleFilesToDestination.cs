// -----------------------------------------------------------------------
//  <copyright file="ISmuggleFiles.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.IO;
using System.Threading.Tasks;

using Raven.Abstractions.FileSystem;

namespace Raven.Smuggler.FileSystem
{
    public interface ISmuggleFilesToDestination : IDisposable
    {
        Task WriteFileAsync(FileHeader file, Stream content);
    }
}