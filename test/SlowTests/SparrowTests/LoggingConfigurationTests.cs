using System;
using System.IO;
using System.Threading.Tasks;
using FastTests;
using Raven.Server.Config;
using Raven.Server.ServerWide;
using Sparrow;
using Sparrow.Json;
using Sparrow.Logging;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.SparrowTests;

public class LoggingConfigurationTests : RavenTestBase
{
    public LoggingConfigurationTests(ITestOutputHelper output) : base(output)
    {
    }

    
    [RavenFact(RavenTestCategory.Logging)]
    public async Task JsonFileModifier_WhenModifyUnexistSettingsFile_ShouldCreateOne()
    {
        var settingJsonPath = NewDataPath();
        
        var settingJsonModifier = new JsonConfigFileModifier(settingJsonPath);
        using (var context = JsonOperationContext.ShortTermSingleUse())
        {
            settingJsonModifier.Execute(context, j =>
            {
                j[RavenConfiguration.GetKey(x => x.Logs.Mode)] = LogMode.Information;
            });
        }
        
        var configuration = RavenConfiguration.CreateForTesting(null, ResourceType.Server, settingJsonPath);
        configuration.Initialize();
        Assert.Equal(LogMode.Information, configuration.Logs.Mode);
    }
    
    [RavenFact(RavenTestCategory.Logging)]
    public async Task JsonFileModifier_WhenModifySettingsWithNoFile_ShouldCreateOne()
    {
        var settingJsonPath = NewDataPath();
        var content = @"
{
    ""Logs.Mode"":""Operation""
}";
        await File.WriteAllTextAsync(settingJsonPath, content);
        
        var settingJsonModifier = new JsonConfigFileModifier(settingJsonPath);
       
        using (var context = JsonOperationContext.ShortTermSingleUse())
        {
            settingJsonModifier.Execute(context, j =>
            {
                j[RavenConfiguration.GetKey(x => x.Logs.Mode)] = LogMode.Information;
            });
        }
        
        var configuration = RavenConfiguration.CreateForTesting(null, ResourceType.Server, settingJsonPath);
        configuration.Initialize();
        Assert.Equal(LogMode.Information, configuration.Logs.Mode);
    }
    
    [RavenFact(RavenTestCategory.Logging)]
    public async Task JsonFileModifier_WhenModifySettingsWithNestedKey_ShouldCreateOne()
    {
        var settingJsonPath = NewDataPath();
        var content = @"
{
    ""Logs.Mode"":""Operation"",
    ""Logs.RetentionTimeInHrs"":100,
    ""Logs.RetentionSizeInMb"":500,
    ""Logs.Compress"":false
}";
        await File.WriteAllTextAsync(settingJsonPath, content);

        var settingJsonModifier = new JsonConfigFileModifier(settingJsonPath);
        using (var context = JsonOperationContext.ShortTermSingleUse())
        {
            settingJsonModifier.Execute(context, j =>
            {
                j[RavenConfiguration.GetKey(x => x.Logs.Mode)] = LogMode.Information;
                j[RavenConfiguration.GetKey(x => x.Logs.RetentionTime)] = 200;
                j[RavenConfiguration.GetKey(x => x.Logs.RetentionSize)] = 600;
                j[RavenConfiguration.GetKey(x => x.Logs.Compress)] = true;
            });
        }
        
        var configuration = RavenConfiguration.CreateForTesting(null, ResourceType.Server, settingJsonPath);
        configuration.Initialize();
        Assert.Equal(LogMode.Information, configuration.Logs.Mode);
        Assert.Equal(TimeSpan.FromHours(200), configuration.Logs.RetentionTime.Value.AsTimeSpan);
        Assert.Equal(new Size(600, SizeUnit.Megabytes), configuration.Logs.RetentionSize);
        Assert.Equal(true, configuration.Logs.Compress);
    }

    [RavenFact(RavenTestCategory.Logging)]
    public async Task JsonFileModifier_WhenOriginLogConfigurationSetAsNested_ShouldOverrideThemAsWell()
    {
        var settingJsonPath = NewDataPath();
        const string content = """
       {
           "Logs": {
               "Mode":"Operation",
               "RetentionTimeInHrs":100,
               "RetentionSizeInMb":500,
               "Compress":false
           }
       }
       """;
        await File.WriteAllTextAsync(settingJsonPath, content);
        
        var settingJsonModifier = new JsonConfigFileModifier(settingJsonPath)
        {
 
        };
        using (var context = JsonOperationContext.ShortTermSingleUse())
        {
            settingJsonModifier.Execute(context, j =>
            {
                j[RavenConfiguration.GetKey(x => x.Logs.Mode)] = LogMode.Information;
                j[RavenConfiguration.GetKey(x => x.Logs.RetentionTime)] = 200;
                j[RavenConfiguration.GetKey(x => x.Logs.RetentionSize)] = 600;
                j[RavenConfiguration.GetKey(x => x.Logs.Compress)] = true;
            });
        }
        
        var configuration = RavenConfiguration.CreateForTesting(null, ResourceType.Server, settingJsonPath);
        configuration.Initialize();
        Assert.Equal(LogMode.Information, configuration.Logs.Mode);
        Assert.Equal(TimeSpan.FromHours(200), configuration.Logs.RetentionTime.Value.AsTimeSpan);
        Assert.Equal(new Size(600, SizeUnit.Megabytes), configuration.Logs.RetentionSize);
        Assert.Equal(true, configuration.Logs.Compress);
    }
}
