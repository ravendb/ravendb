var path = require('path');

var STUDIO_TEST_DIR = '../../test/Studio';

var paths = {
    handlersToParse: [
        '../Raven.Server/**/*Handler.cs'
    ],
    configurationFilesToParse:[
        '../Raven.Server/Config/Categories/**/*Configuration.cs'
    ],
    configurationConstants: '../Raven.Client/Constants.cs',
    constantsTargetDir: './typescript/',
    typingsConfig: './typings.json',
    tsSource: './typescript/**/*.ts',
    typings: './typings/**/*.d.ts',
    tsOutput: './wwwroot/App/',

    aceDir: './wwwroot/Content/ace/',

    test: {
        dir: STUDIO_TEST_DIR,
        tsSource: path.join(STUDIO_TEST_DIR, 'typescript/**/*.ts'),
        tsOutput: path.join(STUDIO_TEST_DIR, 'js'),
        setup: path.join(STUDIO_TEST_DIR, 'setup'),
        html: path.join(STUDIO_TEST_DIR, 'test.html')
    },

    lessSource: [
        './wwwroot/Content/css/styles.less',
        './wwwroot/Content/css/styles-light.less',
        './wwwroot/Content/css/styles-blue.less'
    ],
    lessSourcesToWatch: [
        './wwwroot/Content/css/**/*.less'
    ],
    lessTarget: './wwwroot/Content/',
    lessTargetSelector: './wwwroot/Content/**/*.css',

    releaseTarget: './build/',
    bowerSource: './wwwroot/lib/',
    themeCss: [
        'wwwroot/Content/css/styles.css',
        'wwwroot/Content/css/styles-blue.css',
        'wwwroot/Content/css/styles-light.css'
    ],
    cssToMerge: [
        'wwwroot/lib/eonasdan-bootstrap-datetimepicker/build/css/bootstrap-datetimepicker.css',
        "wwwroot/lib/bootstrap-duration-picker/dist/bootstrap-duration-picker.css",
        'wwwroot/lib/bootstrap-select/dist/css/bootstrap-select.css',
        'wwwroot/lib/bootstrap-multiselect/dist/css/bootstrap-multiselect.css',
        'wwwroot/lib/Durandal/css/durandal.css',
        'wwwroot/lib/animate.css/animate.css',
        'wwwroot/lib/leaflet/dist/leaflet.css',
        'wwwroot/lib/leaflet.markercluster/dist/MarkerCluster.css', 
        'wwwroot/lib/leaflet.markercluster/dist/MarkerCluster.Default.css'
    ],
    externalLibs: [
        'es6-shim/es6-shim.js',
        '../Content/modernizr.js',
        "jquery/dist/jquery.js",
        'lodash/dist/lodash.js',
        'prism/prism.js',
        'Sortable/Sortable.js',
        'prism/components/prism-javascript.js',
        'prism/components/prism-csharp.js',
        "blockUI/jquery.blockUI.js",
        "knockout/dist/knockout.debug.js",
        "knockout-validation/dist/knockout.validation.js",
        "knockout.dirtyFlag/index.js",
        "knockout-delegatedEvents/build/knockout-delegatedEvents.js",
        "knockout-postbox/build/knockout-postbox.js",
        "moment/moment.js",
        "bootstrap/dist/js/bootstrap.js",
        "eonasdan-bootstrap-datetimepicker/build/js/bootstrap-datetimepicker.min.js",
        "bootstrap-duration-picker/dist/bootstrap-duration-picker.js",
        "bootstrap-contextmenu/bootstrap-contextmenu.js",
        "bootstrap-multiselect/dist/js/bootstrap-multiselect.js",
        "bootstrap-select/dist/js/bootstrap-select.js",
        "jwerty/jwerty.js",
        "jquery-fullscreen/jquery.fullscreen.js",
        "spin.js/spin.js",
        "google-analytics/index.js",
        "qrcode.js/qrcode.js",
        "css-escape/css.escape.js",
        "cronstrue/dist/cronstrue.min.js",
        "favico.js/favico.js",
        "leaflet/dist/leaflet-src.js",
        "leaflet.markercluster/dist/leaflet.markercluster.js"
    ]
};

paths.watchDirectories = [
    paths.tsSource,
    paths.test.tsSource
];

paths.releaseTargetApp = path.join(paths.releaseTarget, 'App');
paths.releaseTargetContent = path.join(paths.releaseTarget, 'Content');
paths.releaseTargetContentCss = path.join(paths.releaseTargetContent, 'css');

module.exports = paths;
