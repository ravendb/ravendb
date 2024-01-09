using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Newtonsoft.Json;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Security;
using Raven.Client.Json.Serialization.NewtonsoftJson.Internal;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Platform;
using Sparrow.Server.Platform.Posix;
using Sparrow.Utils;

namespace Raven.Server.Config;

public class JsonConfigFileModifier : IDisposable
{
    private readonly JsonOperationContext _context;
    private readonly string _path;
    private readonly bool _overwriteWholeFile;
    private BlittableJsonReaderObject _json;

    public DynamicJsonValue DynamicJsonValue => _json.Modifications;

    public static JsonConfigFileModifier Create(JsonOperationContext context, string path, bool overwriteWholeFile = false)
    {
        var obj = new JsonConfigFileModifier(context, path, overwriteWholeFile);
        obj.Initialize();
        return obj;
    }

    protected JsonConfigFileModifier(JsonOperationContext context, string path, bool overwriteWholeFile = false)
    {
        _context = context;
        _path = path;
        _overwriteWholeFile = overwriteWholeFile;
    }

    protected void Initialize()
    {
        _json = ReadBlittableFromFile(_context);
        _json.Modifications = new DynamicJsonValue(_json);
    }
    
    public async Task AsyncExecute()
    {
        var modifiedJsonObj = _context.ReadObject(_json, "modified-settings-json");
        await PersistConfigurationAsync(modifiedJsonObj);
    }

    protected virtual void Validate(string path){}
    
    private BlittableJsonReaderObject ReadBlittableFromFile(JsonOperationContext context)
    {
        var jsonConfig = new DynamicJsonValue();
        if (_overwriteWholeFile == false)
            FillJsonFromFile(jsonConfig);

        var fileName = Path.GetFileName(_path);
        return context.ReadObject(jsonConfig, fileName);
    }

    private void FillJsonFromFile(DynamicJsonValue jsonConfig)
    {
        try
        {
            using var fs = OpenFile(_path, FileMode.Open, FileAccess.ReadWrite);
            var configBuilder = new ConfigurationBuilder();
            configBuilder.AddJsonStream(fs);
            var config = configBuilder.Build();
            var orderConfig = config.AsEnumerable().OrderBy(x => x.Key);
            var dicConfig = new Dictionary<string, object>();
            foreach (var (key, value) in orderConfig)
            {
                if (value == null)
                    continue;
                
                var directKey = key.Replace(':', '.');
                if (dicConfig.TryAdd(directKey, value) == false)
                    continue;

                jsonConfig[directKey] = value;
            }
        }
        catch (FileNotFoundException)
        {
        }
    }

    private static FileStream OpenFile(string path, FileMode fileMode, FileAccess fileAccess, FileOptions options = FileOptions.None)
    {
        //dotnet default in System.IO.FileStream
        const int defaultBufferSize = 4096;
        const FileShare defaultShare = FileShare.Read;
        
        try
        {
            return SafeFileStream.Create(path, fileMode, fileAccess, defaultShare, defaultBufferSize, options);
        }
        catch (Exception e) when (e is UnauthorizedAccessException or SecurityException)
        {
            throw new UnsuccessfulFileAccessException(e, path, fileAccess);
        }
    }

    private async Task PersistConfigurationAsync(BlittableJsonReaderObject json)
    {
        var tempFile = _path + ".tmp";

        await using (var file = OpenFile(tempFile, FileMode.Create, FileAccess.ReadWrite, FileOptions.DeleteOnClose))
        await using (var streamWriter = new StreamWriter(file))
        await using (var writer = new JsonTextWriter(streamWriter))
        await using (var reader = new BlittableJsonReader())
        {
            writer.Formatting = Formatting.Indented;
            reader.Initialize(json);

            await writer.WriteTokenAsync(reader);
                
            await writer.FlushAsync();
            await streamWriter.FlushAsync();
            file.Flush(true);
        }

        Validate(tempFile);
        SwitchTempWithOriginalAndCreateBackup(tempFile);
    }

    private void SwitchTempWithOriginalAndCreateBackup(string tempPath)
    {
        try
        {
            if (File.Exists(_path))
            {
                File.Replace(tempPath, _path, _path + ".bak");
            }
            else
            {
                File.Move(tempPath, _path);
            }
            
            if (PlatformDetails.RunningOnPosix)
                Syscall.FsyncDirectoryFor(_path);
        }
        catch (Exception e) when (e is UnauthorizedAccessException or SecurityException)
        {
            throw new UnsuccessfulFileAccessException(e, _path, FileAccess.Write);
        }
    }

    public void Dispose()
    {
        _json?.Dispose();
    }
}
