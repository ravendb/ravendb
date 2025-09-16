using System;
using System.IO;
using System.IO.Compression;
using System.Text.Json;
using Raven.Client.Extensions.Streams;
using Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Errors;

namespace Raven.Server.Documents.Handlers.Debugging.DebugPackage;

public class DebugPackageReader(ZipArchive debugPackageZip, DebugPackageAnalyzeErrors errors) : IDisposable
{
    public DebugPackageContent ReadPackageEntries()
    {
        var packageContent = new DebugPackageContent();
        
        foreach (var entry in debugPackageZip.Entries)
        {
            if (entry.FullName.EndsWith(".error"))
            {
                using var reader = new StreamReader(entry.Open());
                errors.AddFileError(entry.FullName, reader.ReadToEnd());
                
                continue;
            }

            if (entry.FullName.EndsWith("/"))
            {
                continue;
            }

            if (entry.FullName.EndsWith(".log"))
            {
                continue;// TODO arek
                
                //packageContent.Log = entry.Open();
            }
            
            if (entry.FullName.EndsWith(".txt"))
                continue;
            
            if (entry.FullName.EndsWith(".json") == false)
                throw new InvalidOperationException($"Unexpected file type: {entry.FullName}");
            
            var content = new MemoryStream();

            using (var entryStream = entry.Open())
            {
                entryStream.CopyTo(content);
            }
            
            content.Position = 0;
            
            byte[] readData = content.ReadData();

            content.Position = 0;
                
            var json = JsonDocument.Parse(readData);
            
            if (entry.FullName.StartsWith(ServerWideDebugInfoPackageHandler.ServerWidePrefix + "/"))
            {
                var entryName = entry.FullName.Substring(ServerWideDebugInfoPackageHandler.ServerWidePrefix.Length + 1);
                
                packageContent.ServerEntries.Add(entryName, content, json);
            }
            else
            {
                var nameParts = entry.FullName.Split('/');
                
                var databaseName = nameParts[0];
                var entryName = nameParts[1];
                
                packageContent.ForDatabase(databaseName).Add(entryName, content, json);
            }
        }

        return packageContent;
    }
    
    public void Dispose()
    {
        debugPackageZip.Dispose();
    }
    
    
}
