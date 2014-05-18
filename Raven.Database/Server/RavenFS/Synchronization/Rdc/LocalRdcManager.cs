using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.RavenFS;
using Raven.Database.Server.RavenFS.Storage;
using Raven.Database.Server.RavenFS.Storage.Esent;
using Raven.Database.Server.RavenFS.Synchronization.Rdc.Wrapper;
using Raven.Database.Server.RavenFS.Util;

namespace Raven.Database.Server.RavenFS.Synchronization.Rdc
{
	public class LocalRdcManager
	{
		private static readonly ConcurrentDictionary<string, ReaderWriterLockSlim> signatureBuidInProgress =
			new ConcurrentDictionary<string, ReaderWriterLockSlim>();

		private readonly SigGenerator _sigGenerator;

		private readonly ISignatureRepository _signatureRepository;
		private readonly ITransactionalStorage _transactionalStorage;

		public LocalRdcManager(ISignatureRepository signatureRepository, ITransactionalStorage transactionalStorage,
							   SigGenerator sigGenerator)
		{
			_signatureRepository = signatureRepository;
			_transactionalStorage = transactionalStorage;
			_sigGenerator = sigGenerator;
		}

		public Task<SignatureManifest> GetSignatureManifestAsync(DataInfo dataInfo)
		{
			return Task.Factory.StartNew(() =>
			{
				signatureBuidInProgress.GetOrAdd(dataInfo.Name, new ReaderWriterLockSlim())
									   .EnterUpgradeableReadLock();
				IEnumerable<SignatureInfo> signatureInfos = null;

				try
				{
					var lastUpdate = _signatureRepository.GetLastUpdate();

					if (lastUpdate == null || lastUpdate < dataInfo.CreatedAt)
					{
						signatureBuidInProgress.GetOrAdd(dataInfo.Name, new ReaderWriterLockSlim())
											   .EnterWriteLock();
						try
						{
							signatureInfos = PrepareSignatures(dataInfo.Name);
						}
						finally
						{
							signatureBuidInProgress.GetOrAdd(dataInfo.Name, new ReaderWriterLockSlim())
												   .ExitWriteLock();
						}
					}
					else
					{
						signatureInfos = _signatureRepository.GetByFileName();
					}
				}
				finally
				{
					signatureBuidInProgress.GetOrAdd(dataInfo.Name, new ReaderWriterLockSlim())
										   .ExitUpgradeableReadLock();
				}

				var result = new SignatureManifest
				{
					FileLength = dataInfo.Length,
					FileName = dataInfo.Name,
					Signatures = SignatureInfosToSignatures(signatureInfos)
				};
				return result;
			});
		}

		public Stream GetSignatureContentForReading(string sigName)
		{
			return _signatureRepository.GetContentForReading(sigName);
		}

		private IEnumerable<SignatureInfo> PrepareSignatures(string filename)
		{
			var input = StorageStream.Reading(_transactionalStorage, filename);
			return _sigGenerator.GenerateSignatures(input, filename, _signatureRepository);
		}

		private static IList<Signature> SignatureInfosToSignatures(IEnumerable<SignatureInfo> signatureInfos)
		{
			var preResult = from item in signatureInfos
							select new Signature
							{
								Length = item.Length,
								Name = item.Name
							};
			return preResult.ToList();
		}
	}
}