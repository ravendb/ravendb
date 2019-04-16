using System;
using System.Runtime.CompilerServices;
using Xunit;

namespace Tests.Infrastructure
{
    class GoogleCloudStorageFact : FactAttribute
    {
        private const string BucketNameEnvironmentVariable = "GOOGLE_CLOUD_STORAGE_BUCKET_NAME";
        private const string GoogleCloudStorageCredentialEnvironmentVariable = "GOOGLE_CLOUD_STORAGE_CREDENTIAL";

        public static string BucketName { get;  private set; }

        public static string CredentialsJson { get; private set; }

        static GoogleCloudStorageFact()
        {
            Environment.SetEnvironmentVariable("GOOGLE_CLOUD_STORAGE_CREDENTIAL","{\n  \"type\": \"service_account\",\n  \"project_id\": \"prefab-phoenix-237012\",\n  \"private_key_id\": \"e6b23b33d06dd37025054cbe8f18e57f081c6d72\",\n  \"private_key\": \"-----BEGIN PRIVATE KEY-----\\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQCgVuKV59aV9UGB\\nx/EHF0rXj4FYK9vESvdjCT5tK6cvcotxWS7tmydvucLJt7H5JqGZRMMYANKGpJw0\\nrVMbZCBDNCnXdaeUte0CnKeO0LHTOFIfE7CZwSWslf1eMf5ABb8EBK4VA0bZvESh\\norVKUvAVei9mK6U5a47dz1hPRp/soddB2lZFVZT9Zgha4XXBKgkA4j+YmoggHp5E\\nh/7r/oNno7pAyRMOTe4ufASaXi73bIWLOHUulvTr1VzZEpOBbELffJyjVsVE0iLZ\\nvGaQiOhJxv+iP+HyFn7VUvGb2rg/EHsBII3TRFmSAPpSkF1EntGeUoaRkTbkB8Fv\\n88Z9K68tAgMBAAECggEAFy/pbZ9M33vLwN5uw0VBjAHTRTJ2giELPJW6IuSEtW5d\\nrwMkL7VX0ybsfepkQPEuvnD8u6xmxZTpiA6mrZfhuIZDJMb9eJqEj3OjIZqZlL+Y\\n1KiuydVuQtKgBO6643xRPt4EZMKIDPXWgP98MybiVGDKmP5n0vF0hCw3i6NdfkVA\\nqhQb8avDphqyILpihE/2HXPtDlTTOXnq+WJOc0uk64AVvx34eMeZGed5hgwNjQxL\\nzMwiI98i4nGM0sKQ0wQbrwq/XOlGsGfY00YKZmzZ4lBl7ISklvPkUhw+tSzSE8OR\\nxm96xthWkONz/OhMI6swJKk0Hr19TiihoIpousHvwQKBgQDTmp1xcvMU3nmar4Aa\\n1EhbZLPO3vj6t+qRiL6X4K8XFSvozldeAZRhrUj/9xdD23nDEAzn+S07nyEY1Jhx\\nPbnxoQDO7iOQuPkCZ/JbV3/latiVtdpQDErw5YRldrbaJM5FjIfI+h7obm2dfmjB\\nw/944GQyAt0UM1HzvrzRnaoN4QKBgQDB+tEsB5drqcsFyS/dIM+7KJlIkNjevlom\\njFjkKHB+Idj1MOwbUkbxZFlgTNonkkMYdKmFS0/MrZh6MaEpt6UCRbl5FyqZdKvj\\n1153ausZ7x3ygi+01Y/TMZrMcp8SsP3QpiRl+cJkzVrbtGwUdcup8Sz2Rp71F1P1\\n7ztTOBzSzQKBgQCm1XWi1+WNo6au7gYLoSl3XVtjFGuriUwY+H6E0nJZuaiIAf/i\\nL06lAHHY1iDysF09DS+Pyeb+5gS7Rgr25ZrwRmeTvzAtT5mMaxYrLD67S9V9nAaH\\nLFn9uof7U3DxZyl2rkj0jTfHPCGSlfLUKssGq6xzvcw+vAO2MMFAZ5WHIQKBgEH7\\n5K06g9p+rm3watSodaEvhTy28N6MF7RnJ1GtI97z+o4lLxt4GGPCg9iLK+YsDaDD\\nBmsuHB3+qxBd03mSW5HzT80OAVKj6lidiSfL+d8fhKtad4nm336yr/p9vvinth8E\\ndPsvQ13wsMhtjhpDm5zaSjYX/bhXUBsnXqHHQ569AoGAKWDwZYXWw5ndJEvjkpt+\\nK17MEiEQ7AqAQCzA5ZprbXldgoHqMbPunFNpkHp0SEJtRTOTYhAtOlnyn9s0iNHG\\n8Otgm1/hnvS749pZT5bQu2GCmeKYimvu+toY9qmzxeDRXWyzujSSffv6TfDnV3Vm\\nN+AzaWz6R7h4fln8X41wEt0=\\n-----END PRIVATE KEY-----\\n\",\n  \"client_email\": \"admin-571@prefab-phoenix-237012.iam.gserviceaccount.com\",\n  \"client_id\": \"100751708134318243172\",\n  \"auth_uri\": \"https://accounts.google.com/o/oauth2/auth\",\n  \"token_uri\": \"https://oauth2.googleapis.com/token\",\n  \"auth_provider_x509_cert_url\": \"https://www.googleapis.com/oauth2/v1/certs\",\n  \"client_x509_cert_url\": \"https://www.googleapis.com/robot/v1/metadata/x509/admin-571%40prefab-phoenix-237012.iam.gserviceaccount.com\"\n}\n",EnvironmentVariableTarget.User);
            BucketName = Environment.GetEnvironmentVariable(BucketNameEnvironmentVariable, EnvironmentVariableTarget.User);
            CredentialsJson = Environment.GetEnvironmentVariable(GoogleCloudStorageCredentialEnvironmentVariable, EnvironmentVariableTarget.User);
        }

        public GoogleCloudStorageFact([CallerMemberName] string memberName = "")
        {
            if (string.IsNullOrWhiteSpace(BucketName))
            {
                Skip = $"Google cloud storage {memberName} tests missing BucketNameEnvironmentVariable.";
                return;
            }

            if (string.IsNullOrWhiteSpace(CredentialsJson))
            {
                Skip = $"Google cloud storage {memberName} tests missing BucketNameEnvironmentVariable.";
                return;
            }
        }
    }
}
