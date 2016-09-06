var path = require('path');

var STUDIO_TEST_DIR = '../../test/Studio';

var paths = {
    handlersToParse: [
        '../Raven.Server/**/*Handler.cs'
    ],
    configurationFilesToParse:[
        '../Raven.Server/Config/Categories/**/*Configuration.cs'
    ],
    constantsTargetDir: './typescript/',
    typingsConfig: './typings.json',
    tsSource: './typescript/**/*.ts',
    typings: './typings/**/*.d.ts',
    tsOutput: './wwwroot/App/',

    test: {
        dir: STUDIO_TEST_DIR,
        tsSource: path.join(STUDIO_TEST_DIR, 'typescript/**/*.ts'),
        tsOutput: path.join(STUDIO_TEST_DIR, 'js'),
        setup: path.join(STUDIO_TEST_DIR, 'setup'),
        html: path.join(STUDIO_TEST_DIR, 'test.html')
    },

    lessSource: [
        './wwwroot/Content/css/styles.less',
        './wwwroot/Content/css/legacy_styles.less',
        './wwwroot/Content/css/bootstrap/bootstrap.less'],
    lessTarget: './wwwroot/Content/',
    lessTargetSelector: './wwwroot/Content/**/*.css',

    releaseTarget: './build/',
    bowerSource: './wwwroot/lib/',
    cssToMerge: [
        'wwwroot/Content/css/bootstrap/bootstrap.css',
        'wwwroot/lib/eonasdan-bootstrap-datetimepicker/build/css/bootstrap-datetimepicker.css',
        'wwwroot/lib/bootstrap-select/dist/css/bootstrap-select.css',
        'wwwroot/lib/bootstrap-multiselect/dist/css/bootstrap-multiselect.css',
        'wwwroot/lib/font-awesome/css/font-awesome.css',
        'wwwroot/lib/Durandal/css/durandal.css',
        'wwwroot/lib/nprogress/nprogress.css',
        'wwwroot/lib/jquery-ui/themes/base-wo-comments/all.css',
        'wwwroot/lib/jquery.dynatree/dist/skin/ui.dynatree.css',
        'wwwroot/lib/nvd3/build/nv.d3.css',
        'wwwroot/lib/animate.css/animate.css',
        'wwwroot/Content/css/styles.css',
        'wwwroot/Content/css/legacy_styles.css'
    ],
    externalLibs: [
        'jquery/dist/jquery.min.js',
        'blockUI/jquery.blockUI.js',
        'nprogress/nprogress.js',
        'knockout/dist/knockout.js',
        'knockout.dirtyFlag/index.js',
        'knockout-delegatedEvents/build/knockout-delegatedEvents.min.js',
        'knockout-postbox/build/knockout-postbox.min.js',
        'moment/min/moment.min.js',
        'bootstrap/dist/js/bootstrap.min.js',
        'eonasdan-bootstrap-datetimepicker/build/js/bootstrap-datetimepicker.min.js',
        'bootstrap-contextmenu/bootstrap-contextmenu.js',
        'bootstrap-multiselect/dist/js/bootstrap-multiselect.js',
        'bootstrap-select/dist/js/bootstrap-select.min.js',
        'jquery-ui/ui/core.js',
        'jquery-ui/ui/widget.js',
        'jquery-ui/ui/widgets/mouse.js',
        'jquery-ui/ui/widgets/resizable.js',
        'jquery.dynatree/dist/jquery.dynatree.min.js',
        'jwerty/jwerty.js',
        'jquery-fullscreen/jquery.fullscreen-min.js',
        'spin.js/spin.min.js'
    ]
};

paths.releaseTargetApp = path.join(paths.releaseTarget, 'App');

module.exports = paths;
