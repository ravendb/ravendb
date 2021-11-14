using Raven.Client.Documents.Smuggler;
using Raven.Client.ServerWide.JavaScript;
using Raven.Server.Config;
using Raven.Server.Config.Categories;
using Raven.Server.Config.Settings;

namespace Raven.Server.Documents.Indexes.Static
{
    public class JavaScriptOptions : IJavaScriptOptions
    {
        public JavaScriptEngineType EngineType { get; set; }
        public bool StrictMode { get; set; }
        public int MaxSteps { get; set; }
        public TimeSetting MaxDuration { get; set; }

        public int TargetContextCountPerEngine { get; set; }

        public int MaxEngineCount { get; set; }

        public JavaScriptOptions(JavaScriptEngineType engineType = JavaScriptEngineType.Jint, bool strictMode = true, int maxSteps = 10_000, TimeSetting? maxDuration = null, int targetContextCountPerEngine = 10, int maxEngineCount = 50)
        {
            maxDuration ??= new TimeSetting(100, TimeUnit.Milliseconds);
            EngineType = engineType;
            StrictMode = strictMode;
            MaxSteps = maxSteps;
            MaxDuration = maxDuration.Value;
            TargetContextCountPerEngine = targetContextCountPerEngine;
            MaxEngineCount = maxEngineCount;
        }

        public JavaScriptOptions(RavenConfiguration configuration)
        {
            var patchingConfig = configuration.Patching;
            var jsConfig = configuration.JavaScript;
            
            EngineType = jsConfig.EngineType;
            StrictMode = patchingConfig.StrictMode ?? jsConfig.StrictMode; // patching is of priority for backward compatibility
            MaxSteps = configuration.Patching.MaxStepsForScript ?? jsConfig.MaxSteps; // patching is of priority for backward compatibility
            MaxDuration = jsConfig.MaxDuration;
            TargetContextCountPerEngine = jsConfig.TargetContextCountPerEngine;
            MaxEngineCount = jsConfig.MaxEngineCount;
        }

        public JavaScriptOptions(IndexingConfiguration indexConfiguration, RavenConfiguration configuration)
        {
            var patchingConfig = configuration.Patching;
            var jsConfig = configuration.JavaScript;
            
            EngineType = indexConfiguration.JsEngineType ?? jsConfig.EngineType;
            StrictMode = indexConfiguration.JsStrictMode ?? patchingConfig.StrictMode ?? jsConfig.StrictMode; // patching is of priority for backward compatibility
            MaxSteps = indexConfiguration.JsMaxSteps ?? configuration.Patching.MaxStepsForScript ?? jsConfig.MaxSteps; // patching is of priority for backward compatibility
            MaxDuration = indexConfiguration.JsMaxDuration ?? jsConfig.MaxDuration;
            TargetContextCountPerEngine = jsConfig.TargetContextCountPerEngine;
            MaxEngineCount = jsConfig.MaxEngineCount;
        }

        public JavaScriptOptions(IJavaScriptOptions indexConfiguration)
        {
            EngineType = indexConfiguration.EngineType;
            StrictMode = indexConfiguration.StrictMode;
            MaxSteps = indexConfiguration.MaxSteps;
            MaxDuration = indexConfiguration.MaxDuration;
        }

        public JavaScriptOptions(JavaScriptOptionsForSmuggler jsOptionsForSmuggler)
        {
            EngineType = jsOptionsForSmuggler.EngineType;
            StrictMode = jsOptionsForSmuggler.StrictMode;
            MaxSteps = jsOptionsForSmuggler.MaxSteps;
            MaxDuration = new TimeSetting(jsOptionsForSmuggler.MaxDuration, TimeUnit.Milliseconds);
        }
    }
}
