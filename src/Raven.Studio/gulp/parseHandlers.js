'use strict';

var through = require('through2');
var path = require('path');
var Set = require('es6-set');
var Map = require('es6-map');

var ravenActions = new Map();
var latestFile;

var HANDLER_PATTERN = "Handler.cs";

module.exports = function parseHandlers(outputFileName) {
    return through.obj(function (inputFile, encoding, callback) {
        latestFile = inputFile;
        callback(null, findHandlerAnnotations(inputFile, ravenActions));
    }, function (cb) {
        var groupedActions = groupRavenActions(ravenActions);
        var outputFile = createDefinitionFile(groupedActions, outputFileName);
        this.push(outputFile);
        cleanup();
        cb();
    });
};

function cleanup() {
    ravenActions.clear();
    latestFile = null;
}

function findHandlerAnnotations(file, ravenActions) {
    var contents = file.contents.toString();
    var handlerName = findHandlerName(file.path);
    var actions = extractRavenActions(contents);

    if (ravenActions.has(handlerName)) {
        let currentActions = ravenActions.get(handlerName);
        actions = new Set([...actions, ...currentActions]);
    }

    ravenActions.set(handlerName, actions);
    return null;
}

function findHandlerName(input) {
    var fileName = path.basename(input);
    if (!fileName.endsWith(HANDLER_PATTERN)) {
        throw new Error("Cannot handle file: " + input);
    }

    return fileName.substring(0, fileName.length - 10);
}

function extractRavenActions(contents) {
    // line format: [RavenAction("/databases/*/docs", "DELETE", "/databases/{databaseName:string}/docs?id={documentId:string}")]
    // or : [RavenAction("/databases/*/notification-center/watch", "GET", SkipUsagesCount = true)]
    var annotationRegexpWoSkipUsagesCount = /\[RavenAction\(\"([^"]+)\", \"([^"]+)\"(,(\s*) \"([^"]+)\")?\)]/g;
    var match;
    var matches = new Set();
    while ((match = annotationRegexpWoSkipUsagesCount.exec(contents))) {
        var url = match[1];
        if (url !== "/") {
            matches.add(url);
        }
    }
    
    var annotationRegexpWithSkipUsagesCount = /\[RavenAction\(\"([^"]+)\", \"([^"]+)\",(\s*)(SkipUsagesCount|IsDebugInformationEndpoint)(\s*)=(\s*)true\)]/g;
    while ((match = annotationRegexpWithSkipUsagesCount.exec(contents))) {
        var url = match[1];
        if (url !== "/") {
            matches.add(url);
        }
    }

    return matches;
}

function groupRavenActions(ravenActions) {
    var groups = new Map();

    ravenActions.forEach(function (urls, handler) {
        urls.forEach(function(url) {
            var groupRegexp = /\/([^\/]+)\/\*\//;
            var match = groupRegexp.exec(url);

            var urlWithoutGroupPrefix = match ? url.substring(3 + match[1].length) : url;
            // group level
            var group = match ? match[1] : "global";
            if (!groups.has(group)) {
                groups.set(group, new Map());
            }

            // handler level
            var handlersMap = groups.get(group);
            if (!handlersMap.has(handler)) {
                handlersMap.set(handler, new Set());
            }

            // url level
            var urlsSet = handlersMap.get(handler);
            urlsSet.add(urlWithoutGroupPrefix);
        });
    });
    return groups;
}

function createDefinitionFile(groupedActions, outputFileName) {

    var typingSource =
        "// This class is autogenerated. Do NOT modify\n\n" + 
        "class endpointConstants {\n";

    groupedActions.forEach(function (handlersMap, groupName) {
        typingSource += "    static " + groupName + " = { \n";

        var handlersMapSize = handlersMap.size;
        var currentHandlerMap = 0;

        handlersMap.forEach(function (urls, handler) {
            currentHandlerMap++;
            var handlerSeparator = handlersMapSize === currentHandlerMap ? "" : ",";

            var handlerLowerCased = handler.charAt(0).toLowerCase() + handler.slice(1);
            typingSource += "        " + handlerLowerCased + ": { \n";

            var urlsLength = urls.size;
            var currentUrl = 0;

            urls.forEach(function (url) {
                currentUrl++;
                var urlSeparator = urlsLength === currentUrl ? "" : ",";
                
                var trimmedUrl = url.endsWith("$") ? url.slice(0, -1) : url;
                typingSource += "            " + urlToFieldName(url) + ": \"" + trimmedUrl + "\"" + urlSeparator + "\n";
                
            });
            typingSource += "        }" + handlerSeparator + " \n";
        });

        typingSource += "    }\n";
    });
    typingSource += "\n}";
    typingSource += "\nexport = endpointConstants;";

    var outputFile = latestFile.clone({ contents: false });
    outputFile.path = path.join(latestFile.base, outputFileName);
    outputFile.contents = new Buffer(typingSource);
    return outputFile;
}

function urlToFieldName(input) {
    var buffer = "";
    var capitalizeNext = false;
    for (var i = 0; i < input.length; i++) {
        var c = input[i];
        if (c === "/" || c === "-") {
            capitalizeNext = true;
        } else {
            if (capitalizeNext) {
                buffer += c.toUpperCase();
                capitalizeNext = false;
            } else {
                buffer += c;
            }
        }
    }
    return buffer.charAt(0).toLowerCase() + buffer.slice(1);
}
