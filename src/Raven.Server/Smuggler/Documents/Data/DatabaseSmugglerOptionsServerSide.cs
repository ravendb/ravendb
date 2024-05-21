using System;
using System.IO.Compression;
using Microsoft.AspNetCore.Http;
using Raven.Client.Documents.Smuggler;
using Raven.Server.Routing;

namespace Raven.Server.Smuggler.Documents.Data
{
    public sealed class DatabaseSmugglerOptionsServerSide : DatabaseSmugglerOptions, IDatabaseSmugglerImportOptions, IDatabaseSmugglerExportOptions
    {
        private DatabaseSmugglerOptionsServerSide()
        {
            //Only for GenerateJsonDeserializationRoutine
        }

        public DatabaseSmugglerOptionsServerSide(AuthorizationStatus authorizationStatus)
        {
            _authorizationStatus = authorizationStatus;
        }

        public bool ReadLegacyEtag { get; set; }

        public string FileName { get; set; }

        private AuthorizationStatus? _authorizationStatus;

        public AuthorizationStatus AuthorizationStatus
        {
            get
            {
                if (_authorizationStatus == null)
                    throw new InvalidOperationException("AuthorizationStatus must be set");

                return _authorizationStatus.Value;
            }
        }

        public bool SkipRevisionCreation { get; set; }
        
        public ExportCompressionAlgorithm? CompressionAlgorithm { get; set; }

        public CompressionLevel? CompressionLevel { get; set; }

        public void SetAuthorizationStatus(AuthorizationStatus authorizationStatus)
        {
            _authorizationStatus = authorizationStatus;
        }

        public static DatabaseSmugglerOptionsServerSide Create(HttpContext httpContext, AuthorizationStatus authorizationStatus)
        {
            var result = new DatabaseSmugglerOptionsServerSide(authorizationStatus);

            foreach (var item in httpContext.Request.Query)
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
                    else if (string.Equals(key, nameof(IncludeArchived), StringComparison.OrdinalIgnoreCase))
                        result.IncludeArchived = bool.Parse(item.Value[0]);
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

        public DatabaseSmugglerExportOptions ToExportOptions()
        {
            return new DatabaseSmugglerExportOptions()
            {
                EncryptionKey = EncryptionKey,
                Collections = Collections,
                IncludeArtificial = IncludeArtificial,
                IncludeArchived = IncludeArchived,
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
