import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

import zipUtils = require("jszip-utils");
import jszip = require("jszip");

class captureClusterStackTracesCommand extends commandBase {

    execute(): JQueryPromise<Array<clusterWideStackTraceResponseItem>> {
        const args = {
            stacktraces: true
        };
        const url = endpoints.global.serverWideDebugInfoPackage.adminDebugClusterInfoPackage + this.urlEncodeArgs(args);
        
        const task = $.Deferred<Array<clusterWideStackTraceResponseItem>>();
        
        zipUtils.getBinaryContent(url, (error, data) => {
            if (error) {
                task.reject(error);
            } else {
                new jszip()
                    .loadAsync(data)
                    .then(value => {
                        const stackPromises = Object.keys(value.files)
                            .map(fileName => this.extractStackTrace(fileName, value.file(fileName)));
                        
                        Promise.all(stackPromises)
                            .then(result => {
                                task.resolve(result);
                            }).catch(error => task.reject(error));
                    })
                    .catch(error => task.reject(error));
            }
        });
        
        task.fail(error => this.reportError("Unable to fetch stack traces", error));
        
        return task;
    }
    
    private extractStackTrace(fileName: string, zipFile: JSZipObject): Promise<clusterWideStackTraceResponseItem> {
        return new Promise((resolve, reject) => {
            const nodeTagRegexp = /\[([A-Z?]{1,4})\]/;
            const nodeTag = fileName.match(nodeTagRegexp)[1];
            zipFile.async("arraybuffer")
                .then(buffer => {
                    new jszip()
                        .loadAsync(buffer)
                        .then(nodeZipContents => {
                            const stackTracesFile = nodeZipContents.folder("server-wide")
                                .file("admin.threads.stack-trace.json");
                            
                            if (stackTracesFile) {
                                stackTracesFile
                                    .async("string")
                                    .then(stacks => {
                                        const stacksAsJson = JSON.parse(stacks);
                                        
                                        if (stacksAsJson.Error) {
                                            reject(stacksAsJson.Error);
                                        } else {
                                            resolve({
                                                NodeTag: nodeTag,
                                                Stacks: stacksAsJson.Results,
                                                Threads: stacksAsJson.Threads,
                                                NodeUrl: null,
                                                Error: undefined
                                            })
                                        }
                                    })
                                    .catch(e => reject(e));
                            } else {
                                resolve({
                                    NodeTag: nodeTag,
                                    Stacks: null,
                                    NodeUrl: null,
                                    Threads: [],
                                    Error: "Unable to find stack traces for given node."
                                });
                            }
                        })
                        .catch(e => reject(e));
                })
                .catch(e => reject(e));
        });
    }
    
    
}

export = captureClusterStackTracesCommand;
