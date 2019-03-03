using System;
using System.Collections.Generic;
using Microsoft.AspNetCore.Http;
using Raven.Client.Documents.Smuggler;
using Raven.Server.Routing;

namespace Raven.Server.Smuggler.Documents.Data
{
    public class DatabaseSmugglerOptionsServerSide : DatabaseSmugglerOptions, IDatabaseSmugglerImportOptions, IDatabaseSmugglerExportOptions
    {
        public DatabaseSmugglerOptionsServerSide()
        {
            Collections = new List<string>();
        }

        public string FileName { get; set; }

        public List<string> Collections { get; set; }

        public AuthorizationStatus AuthorizationStatus { get; set; } = AuthorizationStatus.ValidUser;

        public bool KeepOriginalChangeVector { get; set; }

        public static DatabaseSmugglerOptionsServerSide Create(HttpContext httpContext)
        {
            var result = new DatabaseSmugglerOptionsServerSide();

            foreach (var item in httpContext.Request.Query)
            {
                try
                {
                    var key = item.Key;
                    if (string.Equals(key, nameof(OperateOnTypes), StringComparison.OrdinalIgnoreCase))
                        result.OperateOnTypes = (DatabaseItemType)Enum.Parse(typeof(DatabaseItemType), item.Value[0]);
                    else if (string.Equals(key, nameof(IncludeExpired), StringComparison.OrdinalIgnoreCase))
                        result.IncludeExpired = bool.Parse(item.Value[0]);
                    else if (string.Equals(key, nameof(RemoveAnalyzers), StringComparison.OrdinalIgnoreCase))
                        result.RemoveAnalyzers = bool.Parse(item.Value[0]);
                    else if (string.Equals(key, nameof(TransformScript), StringComparison.OrdinalIgnoreCase))
                        result.TransformScript = Uri.UnescapeDataString(item.Value[0]);
                    else if (string.Equals(key, nameof(MaxStepsForTransformScript), StringComparison.OrdinalIgnoreCase))
                        result.MaxStepsForTransformScript = int.Parse(item.Value[0]);
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
    }
}
