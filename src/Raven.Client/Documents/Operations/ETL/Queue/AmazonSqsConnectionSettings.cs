using System;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.ETL.Queue;

public sealed class AmazonSqsConnectionSettings
{
    internal const string EmulatorUrlEnvironmentVariable = "RAVEN_AMAZON_SQS_EMULATOR_URL";
    
    public Basic Basic { get; set; }

    public bool Passwordless { get; set; }

    [ForceJsonSerialization]
    internal bool UseEmulator { get; set; }

    public bool IsValidConnection()
    {
        if (IsOnlyOneConnectionProvided() == false)
        {
            return false;
        }

        if (Basic != null && Basic.IsValid() == false)
        {
            return false;
        }

        return true;
    }

    private bool IsOnlyOneConnectionProvided()
    {
        int count = 0;

        if (Basic != null)
            count++;
        if (Passwordless)
            count++;
        if (UseEmulator)
            count++;

        return count == 1;
    }

    public DynamicJsonValue ToJson()
    {
        var json = new DynamicJsonValue
        {
            [nameof(Basic)] = Basic == null
                ? null
                : new DynamicJsonValue
                {
                    [nameof(Basic.AccessKey)] = Basic?.AccessKey,
                    [nameof(Basic.SecretKey)] = Basic?.SecretKey,
                    [nameof(Basic.RegionName)] = Basic?.RegionName
                },
            [nameof(Passwordless)] = Passwordless,
            [nameof(UseEmulator)] = UseEmulator
        };

        return json;
    }

    public string GetQueueUrl()
    {
        // this is just static part of the url, dynamic parts are not accessible
        return UseEmulator ? Environment.GetEnvironmentVariable(EmulatorUrlEnvironmentVariable) : "https://queue.amazonaws.com/"; 
    }
}

public class Basic
{
    public string AccessKey { get; set; }

    public string SecretKey { get; set; }

    public string RegionName { get; set; }

    public bool IsValid()
    {
        return string.IsNullOrWhiteSpace(AccessKey) == false &&
               string.IsNullOrWhiteSpace(SecretKey) == false &&
               string.IsNullOrWhiteSpace(RegionName) == false;
    }
}
