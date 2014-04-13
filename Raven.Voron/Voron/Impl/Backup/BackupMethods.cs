// -----------------------------------------------------------------------
//  <copyright file="Backup.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;

namespace Voron.Impl.Backup
{
	public class BackupMethods
	{
        public static event Action<string> OnVoronBackupInformationalEvent;
        public static event Action<string> OnVoronBackupErrorsEvent;

	    public static ConcurrentDictionary<string, List<StackTrace>> callsToInfoNotify = new ConcurrentDictionary<string, List<StackTrace>>();

        //public  event Action<string> OnVoronBackupInformationalEvent;
        //public  event Action<string> OnVoronBackupErrorsEvent;
        public static void VoronBackupInformationalNotify(string voronBackupInfo)
       // public  void VoronBackupInformationalNotify(string voronBackupInfo)
        {
            var informationalEvent = OnVoronBackupInformationalEvent;
            if (informationalEvent != null)
                informationalEvent(voronBackupInfo);
        }
        public static void VoronBackupErrorNotify(string voronBackupError)
      //  public  void VoronBackupErrorNotify(string voronBackupError)
        {
            var errorEvent = OnVoronBackupErrorsEvent;
            if (errorEvent != null)
                errorEvent(voronBackupError);
        }
        public const string Filename = "RavenDB.Voron.Backup";

		public static FullBackup Full = new FullBackup();

		public static IncrementalBackup Incremental = new IncrementalBackup();

	}
}