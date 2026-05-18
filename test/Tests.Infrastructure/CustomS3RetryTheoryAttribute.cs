using System;
using System.Runtime.CompilerServices;
using Newtonsoft.Json;
using Raven.Client.Documents.Operations.Backups;
using xRetry.v3;

namespace Tests.Infrastructure;

public class CustomS3RetryTheoryAttribute : RetryTheoryAttribute, Xunit.v3.IFactAttribute
{
        string Xunit.v3.IFactAttribute.Skip => this.Skip;

    private static readonly S3Settings _s3Settings;

    public static S3Settings S3Settings => new S3Settings(_s3Settings);

    private static readonly string ParsingError;

    static CustomS3RetryTheoryAttribute()
    {
        if (RavenTestHelper.EnvironmentVariables.CustomS3Settings == null)
            return;

        if (string.IsNullOrEmpty(RavenTestHelper.EnvironmentVariables.CustomS3Settings))
            return;

        try
        {
            _s3Settings = JsonConvert.DeserializeObject<S3Settings>(RavenTestHelper.EnvironmentVariables.CustomS3Settings);
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
        skipMessage = CloudAttributeHelper.TestIsMissingCloudCredentialEnvironmentVariable(RavenTestHelper.EnvironmentVariables.CustomS3Settings == null, RavenTestHelper.EnvironmentVariables.CustomS3SettingsKey, ParsingError, _s3Settings, skipIsRunningOnCI: true);
        return string.IsNullOrEmpty(skipMessage) == false;
    }
}
