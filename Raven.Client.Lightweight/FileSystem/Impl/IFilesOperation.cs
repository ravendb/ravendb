using Raven.Abstractions.Data;
using Raven.Abstractions.FileSystem;
using Raven.Json.Linq;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Raven.Client.FileSystem.Impl
{
    internal interface IFilesOperation 
    {
        String Filename { get; set; }
        Task<FileHeader> Execute(IAsyncFilesSession session);
    }
}
