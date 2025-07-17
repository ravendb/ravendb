using System;
using Raven.Client.Documents.Operations.Backups;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Attachments;

public sealed class RetiredAttachmentsDestinationConfiguration : IDynamicJson
{
    public string Identifier { get; set; }
    public bool Disabled { get; set; }
    public S3Settings S3Settings { get; set; }
    public AzureSettings AzureSettings { get; set; }

    public override int GetHashCode()
    {
        var hashCode = new HashCode();
        hashCode.Add(Identifier);
        hashCode.Add(Disabled);
        hashCode.Add(S3Settings);
        hashCode.Add(AzureSettings);

        return hashCode.ToHashCode();
    }

    public override bool Equals(object obj)
    {
        if (ReferenceEquals(null, obj))
            return false;
        if (ReferenceEquals(this, obj))
            return true;
        if (obj.GetType() != GetType())
            return false;
        return Equals((RetiredAttachmentsDestinationConfiguration)obj);
    }

    private bool Equals(RetiredAttachmentsDestinationConfiguration other)
    {
        if (Identifier != other.Identifier)
            return false;
        if (Disabled != other.Disabled)
            return false;

        if (S3Settings != null)
        {
            if (other.S3Settings == null)
                return false;
            if (S3Settings.Equals(other.S3Settings) == false)
                return false;
        }
        if (S3Settings == null && other.S3Settings != null)
        {
            return false;
        }

        if (AzureSettings != null)
        {
            if (other.AzureSettings == null)
                return false;
            if (AzureSettings.Equals(other.AzureSettings) == false)
                return false;
        }
        if (AzureSettings == null && other.AzureSettings != null)
        {
            return false;
        }

        return true;
    }

    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(Identifier)] = Identifier,
            [nameof(Disabled)] = Disabled,
            [nameof(S3Settings)] = S3Settings?.ToJson(),
            [nameof(AzureSettings)] = AzureSettings?.ToJson(),
        };
    }

    internal void AssertConfiguration(string databaseName = null)
    {
        var databaseNameStr = string.IsNullOrEmpty(databaseName) ? string.Empty : $" for database '{databaseName}'";

        if (string.IsNullOrEmpty(Identifier))
            throw new InvalidOperationException($"Identifier{databaseNameStr} must have a value.");
    }
}
