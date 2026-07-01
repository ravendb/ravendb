using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.ETL.Queue;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Session;
using Raven.Server.Utils;
using Voron.Platform.Posix;
using Sparrow.Collections;
using Sparrow.Platform;
using Sparrow.Utils;
using Xunit;
using System.Text;
using Raven.Client.Documents.Operations;
using Raven.Client.ServerWide.Operations;
using Xunit.Sdk;
using ExceptionAggregator = Raven.Server.Utils.ExceptionAggregator;
using System.Runtime.CompilerServices;

namespace Tests.Infrastructure
{
    public static class RavenTestHelper
    {
        public const string SkipIntegrationMessage = "Skipping integration tests.";
        public const string SkipAiIntegrationMessage = "Skipping AI integration tests.";

        public static readonly ParallelOptions DefaultParallelOptions = new ParallelOptions
        {
            MaxDegreeOfParallelism = ProcessorInfo.ProcessorCount * 2
        };

        public static class EnvironmentVariables
        {
            // Test execution / control - env var names
            public const string LicenseEnvName = "RAVEN_LICENSE";
            public const string LicenseDeveloperEnvName = "RAVEN_LICENSE_DEVELOPER";
            public const string LicenseCommunityEnvName = "RAVEN_LICENSE_COMMUNITY";
            public const string LicenseProfessionalEnvName = "RAVEN_LICENSE_PROFESSIONAL";
            public const string IsRunningOnCIEnvName = "RAVEN_IS_RUNNING_ON_CI";
            public const string SkipIntegrationTestsEnvName = "RAVEN_SKIP_INTEGRATION_TESTS";
            public const string SkipAiIntegrationTestsEnvName = "RAVEN_SKIP_AI_INTEGRATION_TESTS";
            public const string RunTestsWithDocsCompressionEnvName = "RAVEN_DOCS_COMPRESSION_TESTS";
            public const string MaxRunningTestsEnvName = "RAVEN_MAX_RUNNING_TESTS";
            public const string WriteRunningTestsToFileEnvName = "RAVEN_WRITE_RUNNING_TESTS_TO_FILE";
            public const string EnableCultureTestsEnvName = "RAVEN_ENABLE_CULTURE_TESTS";
            public const string EnableNightlyBuildTestsEnvName = "RAVEN_ENABLE_NIGHTLY_BUILD_TESTS";
            public const string ForceNightlyBuildTestsEnvName = "RAVEN_FORCE_NIGHTLY_BUILD_TESTS";
            public const string NightlyBuildTestsStartHourEnvName = "RAVEN_NIGHTLY_BUILD_TESTS_START_HOUR";
            public const string NightlyBuildTestsEndHourEnvName = "RAVEN_NIGHTLY_BUILD_TESTS_END_HOUR";
            public const string InterversionTestServerDirEnvName = "RAVEN_INTERVERSIONTEST_SERVER_DIR";

            // ETL / messaging - env var names
            public const string KafkaUrlEnvName = "RAVEN_KAFKA_URL";
            public const string RabbitMqConnectionStringEnvName = "RAVEN_RABBITMQ_CONNECTION_STRING";
            public const string AzureQueueStorageConnectionStringEnvName = "RAVEN_AZURE_QUEUE_STORAGE_CONNECTION_STRING";
            public const string AmazonSqsEmulatorUrlEnvName = AmazonSqsConnectionSettings.EmulatorUrlEnvironmentVariable;
            public const string AzureConnectionStringEnvName = "RAVEN_AZURE_SERVICE_BUS_CONNECTION_STRING";
            public const string AzureConnectionStringAdminEnvName = "RAVEN_AZURE_SERVICE_BUS_ADMIN_CONNECTION_STRING";

            // SQL / external databases - env var names
            public const string MsSqlConnectionStringEnvName = "RAVEN_MSSQL_CONNECTION_STRING";
            public const string MySqlConnectionStringEnvName = "RAVEN_MYSQL_CONNECTION_STRING";
            public const string NpgSqlConnectionStringEnvName = "RAVEN_NPGSQL_CONNECTION_STRING";
            public const string OracleSqlConnectionStringEnvName = "RAVEN_ORACLESQL_CONNECTION_STRING";
            public const string SnowflakeConnectionStringEnvName = "RAVEN_SNOWFLAKE_CONNECTION_STRING";
            public const string SnowflakeTestingBranchEnvName = "RAVEN_SNOWFLAKE_TESTING_BRANCH";
            public const string MongoDbConnectionStringEnvName = "RAVEN_MONGODB_CONNECTION_STRING";
            public const string ElasticSearchNodeUrlsEnvName = "RAVEN_ELASTICSEARCH_NODE_URLS";

            // AI integrations - env var names
            public const string AiIntegrationOpenAiApiKeyEnvName = "RAVEN_AI_INTEGRATION_OPENAI_API_KEY";
            public const string AiIntegrationAzureOpenAiApiKeyEnvName = "RAVEN_AI_INTEGRATION_AZURE_OPENAI_API_KEY";
            public const string AiIntegrationAzureOpenAiDeploymentEndpointEnvName = "RAVEN_AI_INTEGRATION_AZURE_OPENAI_DEPLOYMENT_ENDPOINT";
            public const string AiIntegrationAzureOpenAiDeploymentNameEnvName = "RAVEN_AI_INTEGRATION_AZURE_OPENAI_DEPLOYMENT_NAME";
            public const string AiIntegrationAzureOpenAiChatDeploymentNameEnvName = "RAVEN_AI_INTEGRATION_AZURE_OPENAI_CHAT_DEPLOYMENT_NAME";
            public const string AiIntegrationGoogleApiKeyEnvName = "RAVEN_AI_INTEGRATION_GOOGLE_API_KEY";
            public const string AiIntegrationHuggingFaceApiKeyEnvName = "RAVEN_AI_INTEGRATION_HUGGINGFACE_API_KEY";
            public const string AiIntegrationMistralApiKeyEnvName = "RAVEN_AI_INTEGRATION_MISTRAL_API_KEY";
            public const string AiIntegrationOllamaEmbUriEnvName = "RAVEN_AI_INTEGRATION_OLLAMA_EMB_URI";
            public const string AiIntegrationOllamaChatUriEnvName = "RAVEN_AI_INTEGRATION_OLLAMA_CHAT_URI";
            public const string AiIntegrationVllmApiKeyEnvName = "RAVEN_AI_INTEGRATION_VLLM_API_KEY";
            public const string AiIntegrationVllmEmbEndpointEnvName = "RAVEN_AI_INTEGRATION_VLLM_EMB_ENDPOINT";
            public const string AiIntegrationVllmEmbModelEnvName = "RAVEN_AI_INTEGRATION_VLLM_EMB_MODEL";
            public const string AiIntegrationVllmChatEndpointEnvName = "RAVEN_AI_INTEGRATION_VLLM_CHAT_ENDPOINT";
            public const string AiIntegrationVllmChatModelEnvName = "RAVEN_AI_INTEGRATION_VLLM_CHAT_MODEL";
            public const string AiIntegrationVertexGoogleCredentialsJsonEnvName = "RAVEN_AI_INTEGRATION_VERTEX_GOOGLE_CREDENTIALS_JSON";
            public const string AiIntegrationVertexLocationEnvName = "RAVEN_AI_INTEGRATION_VERTEX_LOCATION";

            // Cloud credentials (non-RAVEN_ prefixed) - env var names
            public const string S3CredentialEnvName = "S3_CREDENTIAL";
            public const string CustomS3SettingsEnvName = "CUSTOM_S3_SETTINGS";
            public const string AzureCredentialEnvName = "AZURE_CREDENTIAL";
            public const string AzureSasTokenCredentialEnvName = "AZURE_SAS_TOKEN_CREDENTIAL";
            public const string GlacierCredentialEnvName = "GLACIER_CREDENTIAL";
            public const string GoogleCloudBucketNameEnvName = "GOOGLE_CLOUD_BUCKET_NAME";
            public const string GoogleCloudCredentialEnvName = "GOOGLE_CLOUD_CREDENTIAL";

            // Voron / test resource analyzer (non-RAVEN_ prefixed) - env var names
            public const string VoronInternalForceUsing32BitsPagerEnvName = "VORON_INTERNAL_ForceUsing32BitsPager";
            public const string TestResourceAnalyzerEnableEnvName = "TEST_RESOURCE_ANALYZER_ENABLE";
            public const string TestResourceAnalyzerSamplingEnvName = "TEST_RESOURCE_ANALYZER_SAMPLING";

            // CI metadata - env var names
            public const string BranchEnvName = "branch";

            // Test execution / control - values
            public static readonly string License;
            public static readonly string LicenseDeveloper;
            public static readonly string LicenseCommunity;
            public static readonly string LicenseProfessional;
            public static readonly bool IsRunningOnCI;
            public static readonly bool SkipIntegrationTests;
            public static readonly bool SkipAiIntegrationTests;
            public static readonly bool RunTestsWithDocsCompression;
            public static readonly int MaxRunningTests;
            public static readonly bool WriteRunningTestsToFile;
            public static readonly bool EnableCultureTests;
            public static readonly bool EnableNightlyBuildTests;
            public static readonly bool ForceNightlyBuildTests;
            public static readonly int NightlyBuildTestsStartHour;
            public static readonly int NightlyBuildTestsEndHour;
            public static readonly string InterversionTestServerDir;

            // Test execution / control - HasXxx
            public static readonly bool HasLicense;
            public static readonly bool HasLicenseDeveloper;
            public static readonly bool HasLicenseCommunity;
            public static readonly bool HasLicenseProfessional;
            public static readonly bool HasIsRunningOnCI;
            public static readonly bool HasSkipIntegrationTests;
            public static readonly bool HasSkipAiIntegrationTests;
            public static readonly bool HasRunTestsWithDocsCompression;
            public static readonly bool HasMaxRunningTests;
            public static readonly bool HasWriteRunningTestsToFile;
            public static readonly bool HasEnableCultureTests;
            public static readonly bool HasEnableNightlyBuildTests;
            public static readonly bool HasForceNightlyBuildTests;
            public static readonly bool HasNightlyBuildTestsStartHour;
            public static readonly bool HasNightlyBuildTestsEndHour;
            public static readonly bool HasInterversionTestServerDir;

            // ETL / messaging - values
            public static readonly string KafkaUrl;
            public static readonly string RabbitMqConnectionString;
            public static readonly string AzureQueueStorageConnectionString;
            public static readonly string AmazonSqsEmulatorUrl;

            // ETL / messaging - HasXxx
            public static readonly bool HasKafkaUrl;
            public static readonly bool HasRabbitMqConnectionString;
            public static readonly bool HasAzureQueueStorageConnectionString;
            public static readonly bool HasAmazonSqsEmulatorUrl;

            // SQL / external databases - values
            public static readonly string MsSqlConnectionString;
            public static readonly string MySqlConnectionString;
            public static readonly string NpgSqlConnectionString;
            public static readonly string OracleSqlConnectionString;
            public static readonly string SnowflakeConnectionString;
            public static readonly string SnowflakeTestingBranch;
            public static readonly string MongoDbConnectionString;
            public static readonly string ElasticSearchNodeUrls;

            // SQL / external databases - HasXxx
            public static readonly bool HasMsSqlConnectionString;
            public static readonly bool HasMySqlConnectionString;
            public static readonly bool HasNpgSqlConnectionString;
            public static readonly bool HasOracleSqlConnectionString;
            public static readonly bool HasSnowflakeConnectionString;
            public static readonly bool HasSnowflakeTestingBranch;
            public static readonly bool HasMongoDbConnectionString;
            public static readonly bool HasElasticSearchNodeUrls;

            // AI integrations - values
            public static readonly string AiIntegrationOpenAiApiKey;
            public static readonly string AiIntegrationAzureOpenAiApiKey;
            public static readonly string AiIntegrationAzureOpenAiDeploymentEndpoint;
            public static readonly string AiIntegrationAzureOpenAiDeploymentName;
            public static readonly string AiIntegrationAzureOpenAiChatDeploymentName;
            public static readonly string AiIntegrationGoogleApiKey;
            public static readonly string AiIntegrationHuggingFaceApiKey;
            public static readonly string AiIntegrationMistralApiKey;
            public static readonly string AiIntegrationOllamaEmbUri;
            public static readonly string AiIntegrationOllamaChatUri;
            public static readonly string AiIntegrationVllmApiKey;
            public static readonly string AiIntegrationVllmEmbEndpoint;
            public static readonly string AiIntegrationVllmEmbModel;
            public static readonly string AiIntegrationVllmChatEndpoint;
            public static readonly string AiIntegrationVllmChatModel;
            public static readonly string AiIntegrationVertexGoogleCredentialsJson;
            public static readonly string AiIntegrationVertexLocation;

            // AI integrations - HasXxx
            public static readonly bool HasAiIntegrationOpenAiApiKey;
            public static readonly bool HasAiIntegrationAzureOpenAiApiKey;
            public static readonly bool HasAiIntegrationAzureOpenAiDeploymentEndpoint;
            public static readonly bool HasAiIntegrationAzureOpenAiDeploymentName;
            public static readonly bool HasAiIntegrationAzureOpenAiChatDeploymentName;
            public static readonly bool HasAiIntegrationGoogleApiKey;
            public static readonly bool HasAiIntegrationHuggingFaceApiKey;
            public static readonly bool HasAiIntegrationMistralApiKey;
            public static readonly bool HasAiIntegrationOllamaEmbUri;
            public static readonly bool HasAiIntegrationOllamaChatUri;
            public static readonly bool HasAiIntegrationVllmApiKey;
            public static readonly bool HasAiIntegrationVllmEmbEndpoint;
            public static readonly bool HasAiIntegrationVllmEmbModel;
            public static readonly bool HasAiIntegrationVllmChatEndpoint;
            public static readonly bool HasAiIntegrationVllmChatModel;
            public static readonly bool HasAiIntegrationVertexGoogleCredentialsJson;
            public static readonly bool HasAiIntegrationVertexLocation;

            // Cloud credentials - values
            public static readonly string S3Credential;
            public static readonly string CustomS3Settings;
            public static readonly string AzureCredential;
            public static readonly string AzureSasTokenCredential;
            public static readonly string GlacierCredential;
            public static readonly string GoogleCloudBucketName;
            public static readonly string GoogleCloudCredential;

            // Cloud credentials - HasXxx
            public static readonly bool HasS3Credential;
            public static readonly bool HasCustomS3Settings;
            public static readonly bool HasAzureCredential;
            public static readonly bool HasAzureSasTokenCredential;
            public static readonly bool HasGlacierCredential;
            public static readonly bool HasGoogleCloudBucketName;
            public static readonly bool HasGoogleCloudCredential;

            // Voron / test resource analyzer - values
            public static readonly bool VoronInternalForceUsing32BitsPager;
            public static readonly bool TestResourceAnalyzerEnable;
            public static readonly bool TestResourceAnalyzerSampling;

            // Voron / test resource analyzer - HasXxx
            public static readonly bool HasVoronInternalForceUsing32BitsPager;
            public static readonly bool HasTestResourceAnalyzerEnable;
            public static readonly bool HasTestResourceAnalyzerSampling;

            // CI metadata - values
            public static readonly string Branch;

            // CI metadata - HasXxx
            public static readonly bool HasBranch;

            static EnvironmentVariables()
            {
                // Test execution / control
                (License, HasLicense) = ParseString(LicenseEnvName);
                (LicenseDeveloper, HasLicenseDeveloper) = ParseString(LicenseDeveloperEnvName);
                (LicenseCommunity, HasLicenseCommunity) = ParseString(LicenseCommunityEnvName);
                (LicenseProfessional, HasLicenseProfessional) = ParseString(LicenseProfessionalEnvName);
                (IsRunningOnCI, HasIsRunningOnCI) = ParseBool(IsRunningOnCIEnvName);
                (SkipIntegrationTests, HasSkipIntegrationTests) = ParseBool(SkipIntegrationTestsEnvName);
                (SkipAiIntegrationTests, HasSkipAiIntegrationTests) = ParseBool(SkipAiIntegrationTestsEnvName);
                (RunTestsWithDocsCompression, HasRunTestsWithDocsCompression) = ParseBool(RunTestsWithDocsCompressionEnvName);
                (MaxRunningTests, HasMaxRunningTests) = ParseInt(MaxRunningTestsEnvName);
                (WriteRunningTestsToFile, HasWriteRunningTestsToFile) = ParseBool(WriteRunningTestsToFileEnvName);
                (EnableCultureTests, HasEnableCultureTests) = ParseBool(EnableCultureTestsEnvName);
                (EnableNightlyBuildTests, HasEnableNightlyBuildTests) = ParseBool(EnableNightlyBuildTestsEnvName);
                (ForceNightlyBuildTests, HasForceNightlyBuildTests) = ParseBool(ForceNightlyBuildTestsEnvName);
                (NightlyBuildTestsStartHour, HasNightlyBuildTestsStartHour) = ParseInt(NightlyBuildTestsStartHourEnvName);
                (NightlyBuildTestsEndHour, HasNightlyBuildTestsEndHour) = ParseInt(NightlyBuildTestsEndHourEnvName);
                (InterversionTestServerDir, HasInterversionTestServerDir) = ParseString(InterversionTestServerDirEnvName);

                // ETL / messaging
                (KafkaUrl, HasKafkaUrl) = ParseString(KafkaUrlEnvName);
                (RabbitMqConnectionString, HasRabbitMqConnectionString) = ParseString(RabbitMqConnectionStringEnvName);
                (AzureQueueStorageConnectionString, HasAzureQueueStorageConnectionString) = ParseString(AzureQueueStorageConnectionStringEnvName);
                (AmazonSqsEmulatorUrl, HasAmazonSqsEmulatorUrl) = ParseString(AmazonSqsEmulatorUrlEnvName);

                // SQL / external databases
                (MsSqlConnectionString, HasMsSqlConnectionString) = ParseString(MsSqlConnectionStringEnvName);
                (MySqlConnectionString, HasMySqlConnectionString) = ParseString(MySqlConnectionStringEnvName);
                (NpgSqlConnectionString, HasNpgSqlConnectionString) = ParseString(NpgSqlConnectionStringEnvName);
                (OracleSqlConnectionString, HasOracleSqlConnectionString) = ParseString(OracleSqlConnectionStringEnvName);
                (SnowflakeConnectionString, HasSnowflakeConnectionString) = ParseString(SnowflakeConnectionStringEnvName);
                (SnowflakeTestingBranch, HasSnowflakeTestingBranch) = ParseString(SnowflakeTestingBranchEnvName);
                (MongoDbConnectionString, HasMongoDbConnectionString) = ParseString(MongoDbConnectionStringEnvName);
                (ElasticSearchNodeUrls, HasElasticSearchNodeUrls) = ParseString(ElasticSearchNodeUrlsEnvName);

                // AI integrations
                (AiIntegrationOpenAiApiKey, HasAiIntegrationOpenAiApiKey) = ParseString(AiIntegrationOpenAiApiKeyEnvName);
                (AiIntegrationAzureOpenAiApiKey, HasAiIntegrationAzureOpenAiApiKey) = ParseString(AiIntegrationAzureOpenAiApiKeyEnvName);
                (AiIntegrationAzureOpenAiDeploymentEndpoint, HasAiIntegrationAzureOpenAiDeploymentEndpoint) = ParseString(AiIntegrationAzureOpenAiDeploymentEndpointEnvName);
                (AiIntegrationAzureOpenAiDeploymentName, HasAiIntegrationAzureOpenAiDeploymentName) = ParseString(AiIntegrationAzureOpenAiDeploymentNameEnvName);
                (AiIntegrationAzureOpenAiChatDeploymentName, HasAiIntegrationAzureOpenAiChatDeploymentName) = ParseString(AiIntegrationAzureOpenAiChatDeploymentNameEnvName);
                (AiIntegrationGoogleApiKey, HasAiIntegrationGoogleApiKey) = ParseString(AiIntegrationGoogleApiKeyEnvName);
                (AiIntegrationHuggingFaceApiKey, HasAiIntegrationHuggingFaceApiKey) = ParseString(AiIntegrationHuggingFaceApiKeyEnvName);
                (AiIntegrationMistralApiKey, HasAiIntegrationMistralApiKey) = ParseString(AiIntegrationMistralApiKeyEnvName);
                (AiIntegrationOllamaEmbUri, HasAiIntegrationOllamaEmbUri) = ParseString(AiIntegrationOllamaEmbUriEnvName);
                (AiIntegrationOllamaChatUri, HasAiIntegrationOllamaChatUri) = ParseString(AiIntegrationOllamaChatUriEnvName);
                (AiIntegrationVllmApiKey, HasAiIntegrationVllmApiKey) = ParseString(AiIntegrationVllmApiKeyEnvName);
                (AiIntegrationVllmEmbEndpoint, HasAiIntegrationVllmEmbEndpoint) = ParseString(AiIntegrationVllmEmbEndpointEnvName);
                (AiIntegrationVllmEmbModel, HasAiIntegrationVllmEmbModel) = ParseString(AiIntegrationVllmEmbModelEnvName);
                (AiIntegrationVllmChatEndpoint, HasAiIntegrationVllmChatEndpoint) = ParseString(AiIntegrationVllmChatEndpointEnvName);
                (AiIntegrationVllmChatModel, HasAiIntegrationVllmChatModel) = ParseString(AiIntegrationVllmChatModelEnvName);
                (AiIntegrationVertexGoogleCredentialsJson, HasAiIntegrationVertexGoogleCredentialsJson) = ParseString(AiIntegrationVertexGoogleCredentialsJsonEnvName);
                (AiIntegrationVertexLocation, HasAiIntegrationVertexLocation) = ParseString(AiIntegrationVertexLocationEnvName);

                // Cloud credentials
                (S3Credential, HasS3Credential) = ParseString(S3CredentialEnvName);
                (CustomS3Settings, HasCustomS3Settings) = ParseString(CustomS3SettingsEnvName);
                (AzureCredential, HasAzureCredential) = ParseString(AzureCredentialEnvName);
                (AzureSasTokenCredential, HasAzureSasTokenCredential) = ParseString(AzureSasTokenCredentialEnvName);
                (GlacierCredential, HasGlacierCredential) = ParseString(GlacierCredentialEnvName);
                (GoogleCloudBucketName, HasGoogleCloudBucketName) = ParseString(GoogleCloudBucketNameEnvName);
                (GoogleCloudCredential, HasGoogleCloudCredential) = ParseString(GoogleCloudCredentialEnvName);

                // Voron / test resource analyzer
                (VoronInternalForceUsing32BitsPager, HasVoronInternalForceUsing32BitsPager) = ParseBool(VoronInternalForceUsing32BitsPagerEnvName);
                (TestResourceAnalyzerEnable, HasTestResourceAnalyzerEnable) = ParseBool(TestResourceAnalyzerEnableEnvName);
                (TestResourceAnalyzerSampling, HasTestResourceAnalyzerSampling) = ParseBool(TestResourceAnalyzerSamplingEnvName);

                // CI metadata
                (Branch, HasBranch) = ParseString(BranchEnvName);
            }

            private static (string Value, bool IsSet) ParseString(string key)
            {
                var value = Environment.GetEnvironmentVariable(key);
                return (value, string.IsNullOrWhiteSpace(value) == false);
            }

            private static (bool Value, bool IsSet) ParseBool(string key)
            {
                var raw = Environment.GetEnvironmentVariable(key);
                var isSet = bool.TryParse(raw, out var value);
                return (value, isSet);
            }

            private static (int Value, bool IsSet) ParseInt(string key)
            {
                var raw = Environment.GetEnvironmentVariable(key);
                var isSet = int.TryParse(raw, out var value);
                return (value, isSet);
            }
        }

        private static int _pathCount;

        public static string NewDataPath(string testName, int serverPort, bool forceCreateDir = false)
        {
            testName = testName?.Replace("<", "").Replace(">", "");

            var newDataDir = Path.GetFullPath($".\\Databases\\{testName ?? "TestDatabase"}.{serverPort}-{Interlocked.Increment(ref _pathCount)}");

            if (PlatformDetails.RunningOnPosix)
                newDataDir = PosixHelper.FixLinuxPath(newDataDir);

            if (forceCreateDir && Directory.Exists(newDataDir) == false)
                Directory.CreateDirectory(newDataDir);

            return newDataDir;
        }

        public static void AssertTrue(bool condition, Func<string> messageOnFailure)
        {
            string failureMessage = null;
            if (condition == false)
            {
                try
                {
                    failureMessage = messageOnFailure();
                }
                catch (Exception e)
                {
                    failureMessage = e.ToString();
                }
            }

            Assert.True(condition, failureMessage);
        }

        public static void DeletePaths(ConcurrentSet<string> pathsToDelete, ExceptionAggregator exceptionAggregator)
        {
            var localPathsToDelete = pathsToDelete.ToArray();
            foreach (var pathToDelete in localPathsToDelete)
            {
                pathsToDelete.TryRemove(pathToDelete);

                FileAttributes pathAttributes;
                try
                {
                    pathAttributes = File.GetAttributes(pathToDelete);
                }
                catch (FileNotFoundException)
                {
                    continue;
                }
                catch (DirectoryNotFoundException)
                {
                    continue;
                }

                if (pathAttributes.HasFlag(FileAttributes.Directory))
                    exceptionAggregator.Execute(() => ClearDatabaseDirectory(pathToDelete));
                else
                    exceptionAggregator.Execute(() => IOExtensions.DeleteFile(pathToDelete));
            }
        }

        private static void ClearDatabaseDirectory(string dataDir)
        {
            var isRetry = false;

            while (true)
            {
                try
                {
                    IOExtensions.DeleteDirectory(dataDir);
                    break;
                }
                catch (IOException)
                {
                    if (isRetry)
                        throw;

                    GC.Collect();
                    GC.WaitForPendingFinalizers();
                    isRetry = true;

                    Thread.Sleep(200);
                }
            }
        }

        public static IndexQuery GetIndexQuery<T>(IQueryable<T> queryable)
        {
            var inspector = (IRavenQueryInspector)queryable;
            return inspector.GetIndexQuery(isAsync: false);
        }

        public static void AssertNoIndexErrors(IDocumentStore store, string databaseName = null)
        {
            databaseName ??= store.Database;
            var executor = store.Maintenance.ForDatabase(databaseName);
            var databaseRecord = executor.Server.Send(new GetDatabaseRecordOperation(databaseName));

            if (databaseRecord.IsSharded)
            {
                foreach (var shardNumber in databaseRecord.Sharding.Shards.Keys)
                {
                    var shardExecutor = executor.ForShard(shardNumber);
                    AssertNoIndexErrorsInternal(shardExecutor);
                }

                return;
            }

            AssertNoIndexErrorsInternal(executor);

            static void AssertNoIndexErrorsInternal(MaintenanceOperationExecutor executor)
            {
                var errors = executor.Send(new GetIndexErrorsOperation());
                StringBuilder sb = null;
                foreach (var indexErrors in errors)
                {
                    if (indexErrors?.Errors == null || indexErrors.Errors.Length == 0)
                        continue;

                    sb ??= new StringBuilder();

                    sb.AppendLine($"Index Errors for '{indexErrors.Name}' ({indexErrors.Errors.Length})");
                    foreach (var indexError in indexErrors.Errors)
                    {
                        sb.AppendLine($"- {indexError}");
                    }
                    sb.AppendLine();
                }

                if (sb == null)
                    return;

                throw new InvalidOperationException(sb.ToString());
            }
        }

        public static void AssertSetEqualsRespectingNewLines(HashSet<string> set1, HashSet<string> set2)
        {
            var convertedSet1 = set1.Select(ConvertRespectingNewLines).ToHashSet();
            var convertedSet2 = set2.Select(ConvertRespectingNewLines).ToHashSet();
            Assert.True(convertedSet1.SetEquals(convertedSet2));
        }

        public static void AssertEqualRespectingNewLines(string expected, string actual)
        {
            var convertedExpected = ConvertRespectingNewLines(expected);
            var convertedActual = ConvertRespectingNewLines(actual);
            Assert.Equal(convertedExpected, convertedActual);
        }

        public static void AssertNotEqualRespectingNewLines(string expected, string actual)
        {
            var convertedExpected = ConvertRespectingNewLines(expected);
            var convertedActual = ConvertRespectingNewLines(actual);
            Assert.NotEqual(convertedExpected, convertedActual);
        }

        public static void AssertStartsWithRespectingNewLines(string expected, string actual)
        {
            var convertedExpected = ConvertRespectingNewLines(expected);
            var convertedActual = ConvertRespectingNewLines(actual);
            Assert.StartsWith(convertedExpected, convertedActual);
        }

        public static void AssertContainsRespectingNewLines(string expected, string actual)
        {
            var convertedExpected = ConvertRespectingNewLines(expected);
            var convertedActual = ConvertRespectingNewLines(actual);

            Assert.Contains(convertedExpected, convertedActual);
        }

        public static void AreEquivalent<T>(IEnumerable<T> expected, IEnumerable<T> actual)
        {
            var forMonitor = actual.ToList();
            Assert.All(expected, e =>
            {
                Assert.Contains(e, forMonitor);
                forMonitor.Remove(e);
            });
            Assert.Empty(forMonitor);
        }

        public static void AssertAll(Func<string> massageFactory, params Action[] asserts)
        {
            try
            {
                Assert.All(asserts, assert => assert());
            }
            catch (Exception e)
            {
                throw new XunitException(massageFactory() + Environment.NewLine + e.Message);
            }
        }

        public static void AssertAll(params Action[] asserts)
        {
            Assert.All(asserts, assert => assert());
        }
        public static async Task AssertAllAsync(Func<Task<string>> massageFactory, params Action[] asserts)
        {
            try
            {
                Assert.All(asserts, assert => assert());
            }
            catch (Exception e)
            {
                throw new XunitException(await massageFactory() + Environment.NewLine + e.Message);
            }
        }

        private static string ConvertRespectingNewLines(string toConvert)
        {
            if (string.IsNullOrEmpty(toConvert))
                return toConvert;

            var regex = new Regex("\r*\n");
            return regex.Replace(toConvert, Environment.NewLine);
        }

        public static DateTime UtcToday
        {
            get
            {
                return DateTime.UtcNow.Date;
            }
        }

        public class DateTimeComparer : IEqualityComparer<DateTime>
        {
            public static readonly DateTimeComparer Instance = new DateTimeComparer();
            public bool Equals(DateTime x, DateTime y)
            {
                if (x.Kind == DateTimeKind.Local)
                    x = x.ToUniversalTime();

                if (y.Kind == DateTimeKind.Local)
                    y = y.ToUniversalTime();

                return x == y;
            }

            public int GetHashCode(DateTime obj)
            {
                return obj.GetHashCode();
            }
        }

        public static void AssertNotRunningOnCi([CallerMemberName] string caller = null)
        {
            if (EnvironmentVariables.IsRunningOnCI)
                throw new InvalidOperationException($"Operation '{caller}' is forbidden, because tests are running on CI.");
        }

        internal static HashSet<(string Method, string Path)> ServerEndpointsToIgnore =
        [
            ("POST", "/admin/replication/conflicts/solver"),                          // access handled internally
            ("POST", "/setup/dns-n-cert"),                                            // only available in setup mode
            ("POST", "/setup/user-domains"),                                          // only available in setup mode
            ("POST", "/setup/populate-ips"),                                          // only available in setup mode
            ("GET", "/setup/parameters"),                                             // only available in setup mode
            ("GET", "/setup/ips"),                                                    // only available in setup mode
            ("POST", "/setup/hosts"),                                                 // only available in setup mode
            ("POST", "/setup/unsecured"),                                             // only available in setup mode
            ("POST", "/setup/unsecured/package"),                                     // only available in setup mode
            ("POST", "/setup/continue/unsecured"),                                    // only available in setup mode
            ("POST", "/setup/secured"),                                               // only available in setup mode
            ("GET", "/setup/letsencrypt/agreement"),                                  // only available in setup mode
            ("POST", "/setup/letsencrypt"),                                           // only available in setup mode
            ("POST", "/setup/continue/extract"),                                      // only available in setup mode
            ("POST", "/setup/continue"),                                              // only available in setup mode
            ("POST", "/setup/finish"),                                                // only available in setup mode
            ("POST", "/server/notification-center/dismiss"),                          // access handled internally
            ("POST", "/server/notification-center/postpone"),                         // access handled internally
            ("GET", "/admin/debug/cluster-info-package"),                             // heavy
            ("GET", "/admin/debug/remote-cluster-info-package"),                      // heavy
            ("GET", "/admin/debug/info-package"),                                     // heavy
            ("GET", "/admin/debug/threads/contention"),                               // heavy
            ("GET", "/admin/debug/gcdump"),                                           // heavy
            ("GET", "/admin/debug/threads/stack-trace"),                              // heavy
            ("GET", "/admin/debug/memory/gc-events"),                                 // heavy
            ("GET", "/admin/debug/memory/allocations"),                               // heavy
            ("GET", "/license/support"),                                              // heavy 
            ("GET", "/admin/debug/threads/runaway"),                                  // heavy
            ("POST", "/license/free/send-verification-code"),                         // only available in setup mode
            ("POST", "/license/free/download"),                                       // only available in setup mode
         ];

        internal static HashSet<(string Method, string Path)> DatabaseEndpointsToIgnore =
        [
            ("POST", "/databases/*/admin/pull-replication/generate-certificate"),     // heavy
            ("POST", "/databases/*/studio/sample-data")                               // heavy
        ];
    }
}
