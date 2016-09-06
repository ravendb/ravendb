var studioWwwrootPrefix = "../../src/Raven.Studio/wwwroot/";

var require = {
    paths: {
        'jquery': studioWwwrootPrefix + 'lib/jquery/dist/jquery',
        'chai': '../../src/Raven.Studio/wwwroot/lib/chai/chai',
        'Squire': '../../src/Raven.Studio/wwwroot/lib/Squire.js/src/Squire',
        'utils': 'js/utils',
        "moment": studioWwwrootPrefix + "lib/moment/moment",
        'ace': studioWwwrootPrefix + 'Content/ace',

        'endpoints': studioWwwrootPrefix + 'App/endpoints',
        'text': studioWwwrootPrefix + 'lib/requirejs-text/text',
        'viewmodels': studioWwwrootPrefix + 'App/viewmodels/',
        'models': studioWwwrootPrefix + 'App/models/',
        'common': studioWwwrootPrefix + 'App/common/',
        'commands': studioWwwrootPrefix + 'App/commands/',
        'durandal': studioWwwrootPrefix + 'lib/Durandal/js',
        'plugins': studioWwwrootPrefix + 'lib/Durandal/js/plugins/',
        'src/Raven.Studio/typescript': studioWwwrootPrefix + "App/",
        'src/Raven.Studio/wwwroot': studioWwwrootPrefix,
        
        'mocks': 'js/mocks'
    },
    map: {
        '*': {
            'forge': '../../src/Raven.Studio/wwwroot/lib/forge/js/forge',
        }

    }
};