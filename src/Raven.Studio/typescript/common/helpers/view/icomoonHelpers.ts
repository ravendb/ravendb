
class icomoonHelpers {

    /**
     * When adding items here please also define mapping in webpack.config.js > 'webfonts-loader' section. 
     */
    static fixedCodepoints = {
        lock: 0xf101,
        "node-leader": 0xf102,
        placeholder: 0xf103,
        "dbgroup-member": 0xf104,
        "dbgroup-promotable": 0xf105,
        "dbgroup-rehab": 0xf106,
        "server-wide-backup": 0xf107,
        backup2: 0xf108,
        "ravendb-etl": 0xf109,
        "external-replication": 0xf10A,
        "sql-etl": 0xf10B,
        "olap-etl": 0xf10C,
        "elastic-search-etl": 0xf10D,
        "subscription": 0xf10E,
        "pull-replication-hub": 0xf10F,
        "pull-replication-agent": 0xf110,
        "copy-to-clipboard": 0xf111,
        "unfold": 0xf112,
        "database": 0xf113,
        "arrow-down": 0xf114,
        "arrow-right": 0xf115,
        "edit": 0xf116,
        "cancel": 0xf117,
        "warning": 0xf118,
        "default": 0xf119,
        "server": 0xf11A,
        "check": 0xf11B,
        "document": 0xf11C,
        "trash": 0xf11D,
        "info": 0xf11E,
        "danger": 0xf11F,
        "connection-lost": 0xf120,
        "empty-set": 0xf121,
        "disabled": 0xf122,
        "conflicts": 0xf123,
        "waiting": 0xf124,
        "cluster-member": 0xf125,
        "cluster-promotable": 0xf126,
        "cluster-watcher": 0xf127
        
    } as const;
    
    static getCodePointForCanvas(iconName: keyof typeof icomoonHelpers.fixedCodepoints): string {
        const codePoint = icomoonHelpers.fixedCodepoints[iconName];
        if (!codePoint) {
            console.log("Unable to find code point for: " + iconName);
            return icomoonHelpers.getCodePointForCanvas("placeholder");
        }
        
        return "&#x" + codePoint.toString(16) + ";";
    }
}

export = icomoonHelpers;
