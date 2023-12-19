using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
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

public class JsonConfigFileModifier
{
    private readonly string _path;
    private readonly bool _reset;

    public JsonConfigFileModifier(string path, bool reset = false)
    {
        _path = path;
        _reset = reset;
    }

    public void Execute(JsonOperationContext context, Action<DynamicJsonValue> modifyAction)
    {
        using var json = ReadBlittableFromFile(context);
        json.Modifications = new DynamicJsonValue(json);
        modifyAction(json.Modifications);
        var modifiedJsonObj = context.ReadObject(json, "modified-settings-json");
        PersistConfiguration(modifiedJsonObj);
    }

    private BlittableJsonReaderObject ReadBlittableFromFile(JsonOperationContext context)
    {
        var jsonConfig = new DynamicJsonValue();
        if (_reset == false)
            ReadConfigFile(jsonConfig);

        var fileName = Path.GetFileName(_path);
        return context.ReadObject(jsonConfig, fileName);
    }

    private void ReadConfigFile(DynamicJsonValue jsonConfig)
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
                if(value == null)
                    continue;
                
                var directKey = key.Replace(':', '.');
                if(dicConfig.TryAdd(directKey, value) == false)
                    continue;

                jsonConfig[directKey] = value;
            }
        }
        catch (FileNotFoundException)
        {
        }
    }

    private static FileStream OpenFile(string path, FileMode fileMode, FileAccess fileAccess)
    {
        try
        {
            return SafeFileStream.Create(path, fileMode, fileAccess);
        }
        catch (Exception e) when (e is UnauthorizedAccessException or SecurityException)
        {
            throw new UnsuccessfulFileAccessException(e, path, fileAccess);
        }
    }

    private void PersistConfiguration(BlittableJsonReaderObject json)
    {
        using var tempFile = new TempFile(_path);
        
        using (var file = OpenFile(tempFile.Path, FileMode.Create, FileAccess.ReadWrite))
        using (var streamWriter = new StreamWriter(file))
        using (var writer = new JsonTextWriter(streamWriter))
        using (var reader = new BlittableJsonReader())
        {
            writer.Formatting = Formatting.Indented;
            reader.Initialize(json);

            writer.WriteToken(reader);
                
            writer.Flush();
            streamWriter.Flush();
            file.Flush(true);
        }
        
        SwitchTempWithOriginalAndCreateBackup(tempFile.Path);
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

    private class TempFile : IDisposable
    {
        public TempFile(string path)
        {
            Path = path + ".tmp";
        }

        public string Path { get; }

        public void Dispose()
        {
            try
            {
                File.Delete(Path);
            }
            catch
            {
                // ignored
            }
        }
    }
}
