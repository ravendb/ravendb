using System;
using System.IO;
using Lextm.SharpSnmpLib;
using Sparrow.Platform;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public sealed class ProcessOpenFileDescriptors() : ScalarObjectBase<Integer32>(SnmpOids.Server.ProcessOpenFileDescriptors)
    {
        protected override Integer32 GetData()
        {
            if (PlatformDetails.RunningOnLinux == false)
                return new Integer32(-1);
            
            try
            {
                var processId = Environment.ProcessId;
                var fdPath = $"/proc/{processId}/fd";
                
                if (Directory.Exists(fdPath))
                {
                    var fdCount = Directory.GetFileSystemEntries(fdPath).Length;
                    return new Integer32(fdCount);
                }
                
                return Integer32.Zero;
            }
            catch (Exception)
            {
                // If we can't read the file descriptors, return -1
                return new Integer32(-1);
            }
        }
    }
}
