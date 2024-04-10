using System;
using System.Runtime.CompilerServices;
using Newtonsoft.Json;
using Raven.Client.Documents.Operations.Backups;
using xRetry;

namespace Tests.Infrastructure;

public class CustomS3RetryTheoryAttribute : RetryTheoryAttribute
{
    private const string S3CredentialEnvironmentVariable = "CUSTOM_S3_SETTINGS";

    private static readonly S3Settings _s3Settings;

    public static S3Settings S3Settings => new S3Settings(_s3Settings);

    private static readonly string ParsingError;

    private static readonly bool EnvVariableMissing;

    static CustomS3RetryTheoryAttribute()
    {
        var strSettings = Environment.GetEnvironmentVariable(S3CredentialEnvironmentVariable);
        if (strSettings == null)
        {
            EnvVariableMissing = true;
            return;
        }

        if (string.IsNullOrEmpty(strSettings))
            return;

        try
        {
            _s3Settings = JsonConvert.DeserializeObject<S3Settings>(strSettings);
        }
        catch (Exception e)
        {
            ParsingError = e.ToString();
        }
    }

    public CustomS3RetryTheoryAttribute()
    {
    }

    public CustomS3RetryTheoryAttribute([CallerMemberName] string memberName = "", int maxRetries = 3, int delayBetweenRetriesMs = 0, params Type[] skipOnExceptions)
        : base(maxRetries, delayBetweenRetriesMs, skipOnExceptions)
    {
    }

    public override string Skip
    {
        get
        {
            return AzureRetryTheoryAttribute.TestIsMissingCloudCredentialEnvironmentVariable(EnvVariableMissing, S3CredentialEnvironmentVariable, ParsingError, _s3Settings, skipIsRunningOnCICheck: true);
        }

        set => base.Skip = value;
    }
}
