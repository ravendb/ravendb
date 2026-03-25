using System;
using System.Runtime.CompilerServices;
using Newtonsoft.Json;
using Raven.Client.Documents.Operations.Backups;
using xRetry.v3;

namespace Tests.Infrastructure;

public class CustomS3RetryTheoryAttribute : RetryTheoryAttribute, Xunit.v3.IFactAttribute
{
        string Xunit.v3.IFactAttribute.Skip => this.Skip;

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

    public CustomS3RetryTheoryAttribute([CallerMemberName] string memberName = "", int maxRetries = 3, int delayBetweenRetriesMs = 0)
        : base(maxRetries, delayBetweenRetriesMs)
    {
    }

    public new string Skip
    {
        get
        {
            if (string.IsNullOrEmpty(base.Skip) == false)
                return base.Skip;

            ShouldSkip(out var skipMessage);
            return skipMessage;
        }

        set => base.Skip = value;
    }

    private static bool ShouldSkip(out string skipMessage)
    {
        skipMessage = CloudAttributeHelper.TestIsMissingCloudCredentialEnvironmentVariable(EnvVariableMissing, S3CredentialEnvironmentVariable, ParsingError, _s3Settings, skipIsRunningOnCI: true);
        return string.IsNullOrEmpty(skipMessage) == false;
    }
}
