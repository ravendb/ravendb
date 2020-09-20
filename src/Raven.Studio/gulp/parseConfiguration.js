var through = require('through2');
var path = require('path');
var Set = require('es6-set');
var Map = require('es6-map');
var gutil = require('gulp-util');
var PATHS = require('./paths');
var fs = require('fs');

var ravenConfigurations = new Map();
var latestFile;

var HANDLER_PATTERN = "Configuration.cs";
var CONF_CONST_REGEX = /public const string (\w+) = \"([^\"]+)\";/g;


var CLASS_REGEX = /public\s(static\s)?class\s([A-Za-z0-9]+)/g;
var OPEN_CURLY_REGEX = /\{/g;
var CLOSE_CURLY_REGEX = /\}/g;
var CONST_FIELD_REGEX = /public const string ([A-Za-z0-9_]+) = "(.*)";/g;
var CONSTANTS_TOKENS_REGEX =
    new RegExp(`${CLASS_REGEX.source}|${CONST_FIELD_REGEX.source}|${OPEN_CURLY_REGEX.source}|${CLOSE_CURLY_REGEX.source}`, 'g');

var CONFIGURATION_CONSTANTS_FIELDS = (function getConfigurationConstantsFields() {
    var constantsContent = fs.readFileSync(PATHS.configurationConstants, 'utf8');
    var match;
    var classStack = [];
    var classFields = {};
    var result = classFields;
    var curlyCounter = 0;
    var previousMatch = null;
    var classCurlyNumbers = {};
    var currentClassName;

    while (match = CONSTANTS_TOKENS_REGEX.exec(constantsContent)) {
        var fullMatch = match[0];
        if (fullMatch === '{') {
            curlyCounter++;
        } else if (fullMatch === '}') {
            curlyCounter--;

            if (curlyCounter === classCurlyNumbers[currentClassName]) {
                classStack.pop();
                currentClassName = classStack[classStack.length - 1];
                classFields = loadOrCreateClassFields(result, classStack);
            }

        } else if (fullMatch.search(CLASS_REGEX) !== -1) {
            currentClassName = match[2];
            classStack.push(currentClassName);
            classFields = loadOrCreateClassFields(result, classStack);
            classCurlyNumbers[currentClassName] = curlyCounter;

        } else if (fullMatch.search(CONST_FIELD_REGEX) !== -1) {
            classFields[match[3]] = match[4];
        }

        previousMatch = match;
    }

    return result;

    function loadOrCreateClassFields(constants, classStack) {
        return classStack.reduce(function (fields, clazz, i) {
            if (!fields.hasOwnProperty(clazz)) {
                fields[clazz] = {};
            }

            return fields[clazz];
        }, constants);
    }
})();

function getConfigurationConstantValue(key) {
    var keyParts = key.split('.');
    var fieldName = keyParts[keyParts.length - 1];
    var currentKey;
    var result = CONFIGURATION_CONSTANTS_FIELDS;
    while (keyParts.length) {
        currentKey = keyParts.splice(0, 1)[0];

        if (!result.hasOwnProperty(currentKey)) {
            throw new Error(`Field ${key} not found in Configuration constants.`);
        }

        result = result[currentKey];
    }

    return result;
}

function parseConfigurations(outputFileName, constants) {
    return through.obj(function (inputFile, encoding, callback) {
        latestFile = inputFile;
        callback(null, findConfigurationAnnotations(inputFile, ravenConfigurations));
    }, function (cb) {
        var outputFile = createDefinitionFile(ravenConfigurations, outputFileName);
        this.push(outputFile);
        cleanup();
        cb();
    });
};

function cleanup() {
    ravenConfigurations.clear();
    latestFile = null;
}

function findConfigurationAnnotations(file, ravenConfigurations) {
    var contents = file.contents.toString();
    var configurationGroupName = findGroupName(file.path);
    var configKeys = extractSettings(contents);
    if (ravenConfigurations.has(configurationGroupName)) {
        throw new Error("Configuration name clush:" + configurationGroupName);
    }

    ravenConfigurations.set(configurationGroupName, configKeys);

    return null;
}

function findGroupName(input) {
    var fileName = path.basename(input);
    if (!fileName.endsWith(HANDLER_PATTERN)) {
        throw new Error("Cannot handle file: " + input);
    }

    return fileName.substring(0, fileName.length - 16);
}

var CONFIGURATION_ENTRY_REGEX = /\[ConfigurationEntry\(\"?([^"]+)\"?[^)]*\)\]/;
var VALUE_ENTRY_REGEX = /public\s(static\s)?(virtual\s)?([?\w\]\[]+\s)?(\w+)/;

function extractSettings(contents) {
    // line format: [ConfigurationEntry("Databases/ConcurrentResourceLoadTimeoutInSec")]
    var annotationRegexp = new RegExp("(" + CONFIGURATION_ENTRY_REGEX.source + ")|(" + VALUE_ENTRY_REGEX.source + ")", 'g');
    var match;
    var matches = new Map();
    var configurationEntry = null;

    while ((match = annotationRegexp.exec(contents))) {
        var possibleConfigurationEntry = match[2];
        var possibleFieldName = match[7];

        if (!possibleConfigurationEntry && !possibleFieldName) {
            throw new Error("Invalid match: " + match[0]);
        }

        if (possibleConfigurationEntry) {
            if (configurationEntry) {
                continue; // we support multiple ConfigurationEntryAttributes now, but the first one is the 'main key'
            }
            configurationEntry = possibleConfigurationEntry;

        } else if (possibleFieldName) {
            if (!configurationEntry) {
                continue;
            }

            if (/^Constants\./.test(configurationEntry)) {
                configurationEntry = getConfigurationConstantValue(configurationEntry);
            }

            matches.set(possibleFieldName, configurationEntry);

            configurationEntry = null;
        }
    }

    return matches;
}


function createDefinitionFile(configurations, outputFileName) {

    var typingSource =
        "// This class is autogenerated. Do NOT modify\n\n" +
        "class configurationConstants {\n";

    configurations.forEach(function (configMapping, groupName) {
        var groupNameLowerCased = groupName.charAt(0).toLowerCase() + groupName.slice(1);

        typingSource += "    static " + groupNameLowerCased + " = { \n";

        var configMappingSize = configMapping.size;
        var currentConfigMapping = 0;

        configMapping.forEach(function (configKey, fieldName) {
            currentConfigMapping++;
            var configSeparator = currentConfigMapping === configMappingSize ? "" : ",";

            var fieldLowerCased = fieldName.charAt(0).toLowerCase() + fieldName.slice(1);
            typingSource += "        " + fieldLowerCased + ": \"" + configKey + "\"" + configSeparator + "\n";
        });

        typingSource += "    }\n";
    });
    typingSource += "\n}";
    typingSource += "\nexport = configurationConstants;";

    var outputFile = latestFile.clone({ contents: false });
    outputFile.path = path.join(latestFile.base, outputFileName);
    outputFile.contents = new Buffer(typingSource);
    return outputFile;
}

module.exports = parseConfigurations;
