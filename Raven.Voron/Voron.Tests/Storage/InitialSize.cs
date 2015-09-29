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

		public int GetExpectedInitialSize()
		{
//			Win32NativeMethods.SYSTEM_INFO systemInfo;
//			Win32NativeMethods.GetSystemInfo(out systemInfo);

			return 64 * 1024;
		}

		[Fact]
		public void WhenInitialFileSizeIsNotSetTheFileSizeForDataFileAndScratchFileShouldBeSetToSystemAllocationGranularity()
		{


			var options = StorageEnvironmentOptions.ForPath(path);
			options.InitialFileSize = null;

			using (new StorageEnvironment(options))
			{
				var dataFile = Path.Combine(path, Constants.DatabaseFilename);
				var scratchFile = Path.Combine(path, StorageEnvironmentOptions.ScratchBufferName(0));

				Assert.Equal(GetExpectedInitialSize(), new FileInfo(dataFile).Length);
				Assert.Equal(GetExpectedInitialSize(), new FileInfo(scratchFile).Length);
			}
		}

		[Fact]
		public void WhenInitialFileSizeIsSetTheFileSizeForDataFileAndScratchFileShouldBeSetAccordingly()
		{
			var options = StorageEnvironmentOptions.ForPath(path);
			options.InitialFileSize = GetExpectedInitialSize()* 2;

			using (new StorageEnvironment(options))
			{
				var dataFile = Path.Combine(path, Constants.DatabaseFilename);
				var scratchFile = Path.Combine(path, StorageEnvironmentOptions.ScratchBufferName(0));

				Assert.Equal(GetExpectedInitialSize() * 2, new FileInfo(dataFile).Length);
				Assert.Equal(GetExpectedInitialSize() * 2, new FileInfo(scratchFile).Length);
			}
		}

		[Fact]
		public void WhenInitialFileSizeIsSetTheFileSizeForDataFileAndScratchFileShouldBeSetAccordinglyAndItWillBeRoundedToTheNearestGranularity()
		{
			var options = StorageEnvironmentOptions.ForPath(path);
			options.InitialFileSize = GetExpectedInitialSize() * 2 + 1;

			using (new StorageEnvironment(options))
			{
				var dataFile = Path.Combine(path, Constants.DatabaseFilename);
				var scratchFile = Path.Combine(path, StorageEnvironmentOptions.ScratchBufferName(0));

				if (StorageEnvironmentOptions.RunningOnPosix) {
					// on Linux, we use 4K as the allocation granularity
					Assert.Equal (GetExpectedInitialSize ()*2 +4096, new FileInfo (dataFile).Length);
					Assert.Equal (GetExpectedInitialSize ()*2  +4096, new FileInfo (scratchFile).Length);
				} else {
					// on Windows, we use 64K as the allocation granularity
					Assert.Equal (GetExpectedInitialSize () * 3, new FileInfo (dataFile).Length);
					Assert.Equal (GetExpectedInitialSize () * 3, new FileInfo (scratchFile).Length);
				}
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