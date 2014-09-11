// -----------------------------------------------------------------------
//  <copyright file="InitialSize.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.IO;

using Voron.Impl;

using Xunit;

namespace Voron.Tests.Storage
{
	public class InitialSize : StorageTest
	{
		private readonly string path;

		public InitialSize()
		{
			path = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
			if (Directory.Exists(path))
				Directory.Delete(path, true);
		}

		[Fact]
		public void WhenInitialFileSizeIsNotSetTheFileSizeForDataFileAndScratchFileShouldBeSetToSystemAllocationGranularity()
		{
			NativeMethods.SYSTEM_INFO systemInfo;
			NativeMethods.GetSystemInfo(out systemInfo);

			var options = StorageEnvironmentOptions.ForPath(path);
			options.InitialFileSize = null;

			using (new StorageEnvironment(options))
			{
				var dataFile = Path.Combine(path, Constants.DatabaseFilename);
				var scratchFile = Path.Combine(path, StorageEnvironmentOptions.ScratchBufferName(0));

				Assert.Equal(systemInfo.allocationGranularity, new FileInfo(dataFile).Length);
				Assert.Equal(systemInfo.allocationGranularity, new FileInfo(scratchFile).Length);
			}
		}

		[Fact]
		public void WhenInitialFileSizeIsSetTheFileSizeForDataFileAndScratchFileShouldBeSetAccordingly()
		{
			NativeMethods.SYSTEM_INFO systemInfo;
			NativeMethods.GetSystemInfo(out systemInfo);

			var options = StorageEnvironmentOptions.ForPath(path);
			options.InitialFileSize = systemInfo.allocationGranularity * 2;

			using (new StorageEnvironment(options))
			{
				var dataFile = Path.Combine(path, Constants.DatabaseFilename);
				var scratchFile = Path.Combine(path, StorageEnvironmentOptions.ScratchBufferName(0));

				Assert.Equal(systemInfo.allocationGranularity * 2, new FileInfo(dataFile).Length);
				Assert.Equal(systemInfo.allocationGranularity * 2, new FileInfo(scratchFile).Length);
			}
		}

		[Fact]
		public void WhenInitialFileSizeIsSetTheFileSizeForDataFileAndScratchFileShouldBeSetAccordinglyAndItWillBeRoundedToTheNearestGranularity()
		{
			NativeMethods.SYSTEM_INFO systemInfo;
			NativeMethods.GetSystemInfo(out systemInfo);

			var options = StorageEnvironmentOptions.ForPath(path);
			options.InitialFileSize = systemInfo.allocationGranularity * 2 + 1;

			using (new StorageEnvironment(options))
			{
				var dataFile = Path.Combine(path, Constants.DatabaseFilename);
				var scratchFile = Path.Combine(path, StorageEnvironmentOptions.ScratchBufferName(0));

				Assert.Equal(systemInfo.allocationGranularity * 3, new FileInfo(dataFile).Length);
				Assert.Equal(systemInfo.allocationGranularity * 3, new FileInfo(scratchFile).Length);
			}
		}

		public override void Dispose()
		{
			if (!string.IsNullOrEmpty(path) && Directory.Exists(path))
				Directory.Delete(path, true);

			base.Dispose();
		}
	}
}