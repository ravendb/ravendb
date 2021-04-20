class tasksCommonContent {
    
    static readonly responsibleNodeInfo =
        `<ul class="margin-top margin-top-xs no-padding-left margin-left">
            <li><small>The <strong>preferred node</strong> that will handle this task.</small></li>
            <li><small>When this node is down, the cluster selects another node from the Database Group to handle the task.</small></li>
         </ul>`;
        
    static readonly generalBackupInfo =
        "Differences between Backup and Snapshot:" +
        "<ul>" +
            "<li>Data" +
                "<small><ul>" +
                    "<li><strong>Backup</strong> includes documents, indexes definitions and identities.<br> " +
                    "It doesn't include the index data itself, the indexes will be rebuilt after Restore, based on exported definitions.</li>" +
                    "<li><strong>Snapshot</strong> contains the raw data including the indexes (definitions and data).</li>" +
                "</ul></small>" +
            "</li>" +
            "<li>Speed" +
                "<small><ul>" +
                "   <li><strong>Backup</strong> is usually much faster than a <strong>Snapshot</strong></li>" +
                "</ul></small>" +
            "</li>" +
            "<li>Size" +
                "<small><ul>" +
                 "<li><strong>Backup</strong> is much smaller than <strong>Snapshot</strong></li>" +
                "</ul></small>" +
            "</li>" +
            "<li>Restore" +
                "<small><ul>" +
                    "<li>Restore of a <strong>Snapshot</strong> is faster than of a <strong>Backup</strong></li>" +
                "</ul></small>" +
            "</li>" +
        "</ul></>" +
        "Note: An incremental Snapshot is the same as an incremental Backup";
    
    static readonly backupAgeInfo = 
        "<ul>" +
            "<li>Define the minimum time to keep the Backups (and Snapshots) in the system.<br></li>" +
            "<li>A <strong>Full Backup</strong> that is older than the specified retention time will be deleted by RavenDB server.<br>" +
            "If <strong>Incremental Backups</strong> exists, the Full Backup, and its incrementals, are removed only if the <em>last incremental</em> is older than the defined retention time.<br></li>"+
            "<li>The deletion occurs when the backup task is triggered on its schedule.</li>" +
        "</ul>";
 
    static readonly ftpHostInfo =
        "To specify the server protocol, prepend the host with protocol identifier (ftp and ftps are supported).<br />" +
        "If no protocol is specified the default one (ftp://) will be used.<br />" +
        "You can also enter a complete URL e.g. <strong>ftp://host.name:port/backup-folder/nested-backup-folder</strong>";
    
    static readonly serverwideSnapshotEncryptionInfo =
        "When selecting the <strong>'Snapshot'</strong> backup-type, encryption is dependant on the database the task is operating on.<br></li>" +
        "<ul>" +
            "<li>If the database backed up is <strong>encrypted</strong> - the Snapshot will also be encrypted (using the database key).<br></li>" +
            "<li>If the database backed up is <strong>not-encrypted</strong> - the Snapshot will not be encrytped as well.</li>" +
        "</ul>";
    
    static textForPopover(storageName: string, targetOperation: string) : string {
        return `${storageName} should be created manually in order for this ${targetOperation} to work.<br /> ` +
            "You can use the 'Test credentials' button to verify its existance.";
    }

    static textForPopoverGCS(storageName: string): string {
        return `${storageName} should be created manually in order for this backup to work.<br /> ` +
            "You can use the 'Test credentials' button to verify its existance.<br />" +
            "<a href='https://cloud.google.com/storage/docs/bucket-naming' target='_blank'>Bucket naming guidelines</a>";
    }
}

export = tasksCommonContent;
