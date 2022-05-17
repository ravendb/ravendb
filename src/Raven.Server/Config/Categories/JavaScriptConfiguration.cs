using System.ComponentModel;
using Raven.Server.Config.Attributes;
using Raven.Server.Config.Settings;
using Raven.Client.ServerWide.JavaScript;

namespace Raven.Server.Config.Categories
{
    public interface IJavaScriptOptions
    {
        JavaScriptEngineType EngineType { get; set; }
        bool StrictMode { get; set; }
        int MaxSteps { get; set; }
        TimeSetting MaxDuration { get; set; }
        int TargetContextCountPerEngine { get; set; }
        int MaxEngineCount { get; set; }
    }
    
    [ConfigurationCategory(ConfigurationCategoryType.JavaScript)]
    public class JavaScriptConfiguration : ConfigurationCategory, IJavaScriptOptions
    {
        [Description("EXPERT: the type of JavaScript engine that will be used by RavenDB: 'Jint'  or 'V8'")]
        [DefaultValue(JavaScriptEngineType.Jint)]
        [ConfigurationEntry("JsConfiguration.Engine", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public JavaScriptEngineType EngineType { get; set; }

        [Description("EXPERT: Enables Strict Mode in JavaScript engine. Default: true")]
        [DefaultValue(true)]
        [ConfigurationEntry("JsConfiguration.StrictMode", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public bool StrictMode { get; set; }

        [Description("EXPERT: Maximum number of steps in the JS script execution (Jint)")]
        [DefaultValue(10_000)]
        [ConfigurationEntry("JsConfiguration.MaxSteps", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public int MaxSteps { get; set; }

        [Description("EXPERT: Maximum duration in milliseconds of the JS script execution")]
        [DefaultValue(-1)]
        [TimeUnit(TimeUnit.Milliseconds)]
        [ConfigurationEntry("JsConfiguration.MaxDuration", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public TimeSetting MaxDuration { get; set; }
        
        [Description("EXPERT: Target number of contexts per engine (isolate) (V8)")]
        [DefaultValue(10)]
        [ConfigurationEntry("JsConfiguration.TargetContextCountPerEngine", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public int TargetContextCountPerEngine { get; set; }
        
        [Description("EXPERT: Maximum number of engines (isolates) (V8)")]
        [DefaultValue(50)]
        [ConfigurationEntry("JsConfiguration.MaxEngineCount", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public int MaxEngineCount { get; set; }
    }
}
