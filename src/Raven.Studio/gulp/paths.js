var path = require('path');

var paths = {
    handlersToParse: [
        '../Raven.Server/**/*Handler.cs'
    ],
    handlersConstantsTargetDir: './typescript/',
    typingsConfig: './typings.json',
    tsSource: './typescript/**/*.ts',
    typings: './typings/**/*.d.ts',
    tsOutput: './wwwroot/App/',
    tsTestSource: '../../test/Studio/typescript/**/*.ts',
    tsTestOutput: '../../test/Studio/js/',
    lessSource: [
        './wwwroot/Content/app.less',
        './wwwroot/Content/bootstrap.less',
        './wwwroot/Content/dynatree.custom.less',
        './wwwroot/Content/awesome-bootstrap-checkbox.less'],
    lessTarget: './wwwroot/Content/',
    lessTargetSelector: './wwwroot/Content/**/*.css',

    oldLessSource: [
        './wwwroot/Content_old/old_app.less',
        './wwwroot/Content_old/old_bootstrap.less',
        './wwwroot/Content_old/dynatree.custom.less',
        './wwwroot/Content_old/awesome-bootstrap-checkbox.less'],
    oldLessTarget: './wwwroot/Content_old/',
    oldLessTargetSelector: './wwwroot/Content_old/**/*.css',

    releaseTarget: './build/',
    bowerSource: './wwwroot/lib/',
    cssToMerge: [
        'wwwroot/Content/bootstrap.css',
        'wwwroot/lib/eonasdan-bootstrap-datetimepicker/build/css/bootstrap-datetimepicker.css',
        'wwwroot/lib/bootstrap-select/dist/css/bootstrap-select.css',
        'wwwroot/lib/bootstrap-multiselect/dist/css/bootstrap-multiselect.css',
        'wwwroot/lib/font-awesome/css/font-awesome.css',
        'wwwroot/lib/Durandal/css/durandal.css',
        'wwwroot/lib/nprogress/nprogress.css',
        'wwwroot/lib/jquery-ui/themes/base-wo-comments/all.css',
        'wwwroot/lib/jquery.dynatree/dist/skin/ui.dynatree.css',
        'wwwroot/Content/dynatree.custom.css',
        'wwwroot/lib/nvd3/build/nv.d3.css',
        'wwwroot/Content/app.css',
        'wwwroot/lib/animate.css/animate.css'
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
