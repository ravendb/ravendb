
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
        "snowflake-etl": 0xf152,
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
        "cluster-watcher": 0xf127,
        "arrow-up": 0xf128,
        "kafka-etl": 0xf129,
        "rabbitmq-etl": 0xf130,
        "kafka-sink": 0xf131,
        "rabbitmq-sink": 0xf132,
        "preview": 0xf133,
        "azure-queue-storage-etl": 0xf134,
        "corax-include-null-match": 0xf140,
        "corax-fallback": 0xf141,
        "corax-all-entries-match": 0xf142,
        "corax-boosting-match": 0xf143,
        "corax-forward": 0xf144,
        "corax-memoization-match": 0xf145,
        "corax-multi-term-match": 0xf146,
        "corax-operator-and": 0xf147,
        "corax-operator-andnot": 0xf148,
        "corax-operator-or": 0xf149,
        "corax-phrase-query": 0xf14A,
        "corax-sorting-match": 0xf14B,
        "corax-spatial-match": 0xf14C,
        "corax-term-match": 0xf14D,
        "corax-unary-match": 0xf14E,
        "corax-backward": 0xf14F,
        "corax-sort-az": 0xf150,
        "corax-sort-za": 0xf151,
        "close": 0xf162,
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
