using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Amazon;
using Amazon.Glacier;
using Amazon.Glacier.Model;
using Amazon.Runtime;
using Amazon.Runtime.Internal.Util;
using Raven.Client.Documents.Operations.Backups;
using Sparrow;
using Sparrow.Binary;

namespace Raven.Server.Documents.PeriodicBackup.Aws
{
    public class RavenAwsGlacierClient : IDisposable
    {
        internal Size MaxUploadArchiveSize = new Size(256, SizeUnit.Megabytes);
        internal Size MinOnePartUploadSizeLimit = new Size(128, SizeUnit.Megabytes);

        private Size MaxOnePartUploadSizeLimit = new Size(4, SizeUnit.Gigabytes);
        private Size TotalArchiveSizeLimit = new Size(40, SizeUnit.Terabytes);

        private AmazonGlacierClient _client;
        private readonly string _region;
        private readonly string _vaultName;
        private readonly Progress _progress;
        private readonly CancellationToken _cancellationToken;

        public RavenAwsGlacierClient(GlacierSettings glacierSettings, Progress progress = null, CancellationToken cancellationToken = default)
        {
            if (string.IsNullOrWhiteSpace(glacierSettings.AwsAccessKey))
                throw new ArgumentException("AWS Access Key cannot be null or empty");

            if (string.IsNullOrWhiteSpace(glacierSettings.AwsSecretKey))
                throw new ArgumentException("AWS Secret Key cannot be null or empty");

            if (string.IsNullOrWhiteSpace(glacierSettings.VaultName))
                throw new ArgumentException("AWS Vault Name cannot be null or empty");

            if (string.IsNullOrWhiteSpace(glacierSettings.AwsRegionName))
                throw new ArgumentException("AWS Region Name cannot be null or empty");

            var region = RegionEndpoint.GetBySystemName(glacierSettings.AwsRegionName);

            AWSCredentials credentials;
            if (string.IsNullOrWhiteSpace(glacierSettings.AwsSessionToken))
                credentials = new BasicAWSCredentials(glacierSettings.AwsAccessKey, glacierSettings.AwsSecretKey);
            else
                credentials = new SessionAWSCredentials(glacierSettings.AwsAccessKey, glacierSettings.AwsSecretKey, glacierSettings.AwsSessionToken);

            _client = new AmazonGlacierClient(credentials, region);
            _region = glacierSettings.AwsRegionName;
            _vaultName = glacierSettings.VaultName;
            _progress = progress;
            _cancellationToken = cancellationToken;
        }

        public string UploadArchive(Stream stream, string archiveDescription)
        {
            return AsyncHelpers.RunSync(() => UploadArchiveAsync(stream, archiveDescription));
        }

        private async Task<string> UploadArchiveAsync(Stream stream, string archiveDescription)
        {
            await TestConnectionAsync();

            var streamSize = new Size(stream.Length, SizeUnit.Bytes);
            if (streamSize > TotalArchiveSizeLimit)
                throw new InvalidOperationException($@"Can't upload more than 40TB to AWS Glacier, current upload size: {streamSize}");

            var streamLength = streamSize.GetValue(SizeUnit.Bytes);
            try
            {
                _progress?.UploadProgress.SetTotal(streamLength);

                if (streamSize > MaxUploadArchiveSize)
                {
                    var partSize = GetPartSize(streamLength);

                    _progress?.UploadProgress.ChangeType(UploadType.Chunked);

                    var initiateResponse = await _client.InitiateMultipartUploadAsync(new InitiateMultipartUploadRequest
                    {
                        ArchiveDescription = archiveDescription,
                        VaultName = _vaultName,
                        AccountId = "-",
                        PartSize = partSize
                    }, _cancellationToken);

                    var partChecksums = new List<string>();

                    var currentPosition = 0L;
                    while (stream.Position < streamLength)
                    {
                        var partStream = GlacierUtils.CreatePartStream(stream, partSize);
                        var partChecksum = TreeHashGenerator.CalculateTreeHash(partStream);

                        partChecksums.Add(partChecksum);

                        var uploadRequest = new UploadMultipartPartRequest
                        {
                            UploadId = initiateResponse.UploadId,
                            VaultName = _vaultName,
                            AccountId = "-",
                            Body = partStream,
                            StreamTransferProgress = (_, args) => _progress?.UploadProgress.UpdateUploaded(args.IncrementTransferred),
                            Checksum = partChecksum
                        };

                        uploadRequest.SetRange(currentPosition, currentPosition + partStream.Length - 1);

                        await _client.UploadMultipartPartAsync(uploadRequest, _cancellationToken);

                        currentPosition += partStream.Length;
                    }

                    var completeResponse = await _client.CompleteMultipartUploadAsync(new CompleteMultipartUploadRequest
                    {
                        AccountId = "-",
                        VaultName = _vaultName,
                        ArchiveSize = streamLength.ToString(),
                        UploadId = initiateResponse.UploadId,
                        Checksum = TreeHashGenerator.CalculateTreeHash(partChecksums)
                    }, _cancellationToken);

                    return completeResponse.ArchiveId;
                }

                var response = await _client.UploadArchiveAsync(new UploadArchiveRequest
                {
                    AccountId = "-",
                    ArchiveDescription = archiveDescription,
                    Body = stream,
                    VaultName = _vaultName,
                    StreamTransferProgress = (_, args) => _progress?.UploadProgress.UpdateUploaded(args.IncrementTransferred),
                    Checksum = TreeHashGenerator.CalculateTreeHash(stream)
                }, _cancellationToken);

                return response.ArchiveId;
            }
            finally
            {
                _progress?.UploadProgress.ChangeState(UploadState.Done);
            }
        }

        public Task PutVaultAsync()
        {
            return _client.CreateVaultAsync(new CreateVaultRequest
            {
                VaultName = _vaultName,
                AccountId = "-"
            }, _cancellationToken);
        }

        public void TestConnection()
        {
            AsyncHelpers.RunSync(TestConnectionAsync);
        }

        private async Task TestConnectionAsync()
        {
            try
            {
                await _client.DescribeVaultAsync(new DescribeVaultRequest(_vaultName), _cancellationToken);
            }
            catch (ResourceNotFoundException e)
            {
                throw new VaultNotFoundException($"Vault name '{_vaultName}' doesn't exist in {_region}!", e);
            }
        }

        public void Dispose()
        {
            _client?.Dispose();
            _client = null;
        }

        private long GetPartSize(long length)
        {
            // using a chunked upload we can upload up to 10,000 chunks, 4GB max each
            // we limit every chunk to a minimum of 128MB
            // constraints: the part size must be a megabyte(1024KB) 
            // multiplied by a power of 2-for example, 
            // 1048576(1MB), 2097152(2MB), 4194304(4MB), 8388608(8 MB), and so on.
            // the minimum allowable part size is 1MB and the maximum is 4GB(4096 MB).
            var maxLengthPerPart = Math.Max(MinOnePartUploadSizeLimit.GetValue(SizeUnit.Bytes), length / 10000);
            return Math.Min(Bits.PowerOf2(maxLengthPerPart), MaxOnePartUploadSizeLimit.GetValue(SizeUnit.Bytes));
        }
    }
}
