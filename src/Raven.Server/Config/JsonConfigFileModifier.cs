using System;
using System.Collections.Generic;
using System.Diagnostics;
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
using Sparrow.Logging;
using Sparrow.Platform;
using Sparrow.Server.Platform.Posix;
using Sparrow.Utils;

namespace Raven.Server.Config;

public class JsonConfigFileModifier : IDisposable
{
    private static readonly Logger Logger = LoggingSource.Instance.GetLogger<JsonConfigFileModifier>("Server");
    
    private readonly JsonOperationContext _context;
    private readonly string _path;
    private readonly bool _overwriteWholeFile;
    private BlittableJsonReaderObject _originJson;

    public DynamicJsonValue Modifications => _originJson.Modifications;

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
        _originJson = ReadBlittableFromFile(_context);
        _originJson.Modifications = new DynamicJsonValue(_originJson);
    }
    
    public async Task AsyncExecute()
    {
        var modifiedJsonObj = _context.ReadObject(_originJson, "modified-settings-json");
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

    private async Task PersistConfigurationAsync(BlittableJsonReaderObject json)
    {
        var tempFile = _path + ".tmp";

        try
        {
            await using (var file = OpenFile(tempFile, FileMode.Create, FileAccess.ReadWrite))
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
        finally
        {
            try
            {
                File.Delete(tempFile);
            }
            catch
            {
                // ignored
            }
        }
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

    protected bool IsOriginalValue<T>(string key, T value)
    {
        if (_originJson.TryGetMember(key, out object result) == false)
            return false;
        if(BlittableJsonReaderObject.TryConvertType<T>(result, out var tValue) == false)
        {
            var msg = $"Can't compare {value} of type {value.GetType()} with {result} because {result.GetType()} is not convertable to {value.GetType()}";
            Debug.Assert(false, msg);
            if (Logger.IsInfoEnabled)
                Logger.Info(msg);
            return false;
        }

        return value.Equals(tValue);
    }
    
    public void Dispose()
    {
        _originJson?.Dispose();
    }
}
