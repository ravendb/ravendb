using System;
using System.Collections.Generic;
using Microsoft.AspNetCore.Http;
using Raven.Client.Documents.Smuggler;
using Raven.Client.ServerWide.JavaScript;
using Raven.Server.Routing;

namespace Raven.Server.Smuggler.Documents.Data
{
    public class DatabaseSmugglerOptionsServerSide : DatabaseSmugglerOptions, IDatabaseSmugglerImportOptions, IDatabaseSmugglerExportOptions
    {
        public class JavaScriptOptionsOptionsServerSide : JavaScriptOptionsForSmuggler
        {
            public JavaScriptOptionsOptionsServerSide()
            {
            }

            public static JavaScriptOptionsOptionsServerSide Parse(IQueryCollection query)
            {
                var result = new JavaScriptOptionsOptionsServerSide();

                foreach (var item in query)
                {
                    try
                    {
                        var key = item.Key;
                        if (string.Equals(key, nameof(EngineType), StringComparison.OrdinalIgnoreCase))
                            result.EngineType = (JavaScriptEngineType)Enum.Parse(typeof(JavaScriptEngineType), item.Value[0]);
                        else if (string.Equals(key, nameof(StrictMode), StringComparison.OrdinalIgnoreCase))
                            result.StrictMode = bool.Parse(item.Value[0]);
                        else if (string.Equals(key, nameof(MaxSteps), StringComparison.OrdinalIgnoreCase))
                            result.MaxSteps = int.Parse(item.Value[0]);
                        else if (string.Equals(key, nameof(MaxDuration), StringComparison.OrdinalIgnoreCase))
                            result.MaxDuration = int.Parse(item.Value[0]);
                    }
                    catch (Exception e)
                    {
                        throw new ArgumentException($"Could not handle query string parameter '{item.Key}' (value: {item.Value})", e);
                    }
                }

                return result;
            }
        }

        public DatabaseSmugglerOptionsServerSide()
        {
            Collections = new List<string>();
        }

        public bool ReadLegacyEtag { get; set; }

        public string FileName { get; set; }

        public List<string> Collections { get; set; }

        public AuthorizationStatus AuthorizationStatus { get; set; } = AuthorizationStatus.ValidUser;

        public bool SkipRevisionCreation { get; set; }
        
        public static DatabaseSmugglerOptionsServerSide Create(HttpContext httpContext)
        {
            var result = new DatabaseSmugglerOptionsServerSide();

            var query = httpContext.Request.Query;
            result.OptionsForTransformScript = JavaScriptOptionsOptionsServerSide.Parse(query);
            foreach (var item in query)
            {
                try
                {
                    var key = item.Key;
                    if (string.Equals(key, nameof(OperateOnTypes), StringComparison.OrdinalIgnoreCase))
                        result.OperateOnTypes = (DatabaseItemType)Enum.Parse(typeof(DatabaseItemType), item.Value[0]);
                    else if (string.Equals(key, nameof(IncludeExpired), StringComparison.OrdinalIgnoreCase))
                        result.IncludeExpired = bool.Parse(item.Value[0]);
                    else if (string.Equals(key, nameof(IncludeArtificial), StringComparison.OrdinalIgnoreCase))
                        result.IncludeArtificial = bool.Parse(item.Value[0]);
                    else if (string.Equals(key, nameof(RemoveAnalyzers), StringComparison.OrdinalIgnoreCase))
                        result.RemoveAnalyzers = bool.Parse(item.Value[0]);
                    else if (string.Equals(key, nameof(TransformScript), StringComparison.OrdinalIgnoreCase))
                        result.TransformScript = Uri.UnescapeDataString(item.Value[0]);
                    else if (string.Equals(key, "collection", StringComparison.OrdinalIgnoreCase))
                        result.Collections.AddRange(item.Value);
                    else if (string.Equals(key, nameof(SkipRevisionCreation), StringComparison.OrdinalIgnoreCase))
                        result.SkipRevisionCreation = bool.Parse(item.Value[0]);
                }
                catch (Exception e)
                {
                    throw new ArgumentException($"Could not handle query string parameter '{item.Key}' (value: {item.Value})", e);
                }
            }
            return result;
        }

        public DatabaseSmugglerExportOptions ToExportOptions()
        {
            return new DatabaseSmugglerExportOptions()
            {
                EncryptionKey = EncryptionKey,
                Collections = Collections,
                IncludeArtificial = IncludeArtificial,
                IncludeExpired = IncludeExpired,
                MaxStepsForTransformScript = MaxStepsForTransformScript,
                OperateOnDatabaseRecordTypes = OperateOnDatabaseRecordTypes,
                OperateOnTypes = OperateOnTypes,
                RemoveAnalyzers = RemoveAnalyzers,
                TransformScript = TransformScript,
                IsShard = IsShard
            };
        }
    }
}
