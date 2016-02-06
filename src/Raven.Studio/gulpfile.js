/// <binding AfterBuild='dev-compile' ProjectOpened='tsd' />
var gulp = require('gulp'),
    ts = require('gulp-typescript'),
    less = require('gulp-less'),
    tsd = require('gulp-tsd'),
    sourcemaps = require("gulp-sourcemaps");

var paths = {
    tsdConfig: './tsd.json',
    tsSource: './typescript/**/*.ts',
    typings: './typings/**/*.d.ts',
    tsOutput: './wwwroot/App/',
    lessSource: './wwwroot/Content/app.less',
    lessTarget: './wwwroot/Content/'
};

var tsCompilerConfig = ts.createProject('tsconfig.json');

gulp.task('dev-less', function() {
    gulp.src(paths.lessSource)
        .pipe(sourcemaps.init())
        .pipe(less({
            sourceMap: true
        }))
        .pipe(sourcemaps.write("."))
        .pipe(gulp.dest(paths.lessTarget));
});

gulp.task('ts-dev-compile', function() {
    return gulp.src([paths.tsSource, paths.typings])
        .pipe(sourcemaps.init())
        .pipe(ts(tsCompilerConfig))
        .js
        .pipe(sourcemaps.write("."))
        .pipe(gulp.dest(paths.tsOutput));
});

gulp.task('tsd', function(callback) {
    tsd({
        command: 'reinstall',
        config: paths.tsdConfig
    }, callback);
});

gulp.task('dev-compile', ['dev-less', 'ts-dev-compile']);

gulp.task('watch', ['dev-compile'], function () {
    gulp.watch(paths.tsSource, ['dev-compile']);
});